import { TransactionState } from "./Core";
import path from 'path';
import fs from 'mz/fs';
import { Semaphore, SynchronizedBy } from "data-semaphore";
import { dynamic, count, AsyncStream, map, dropErrors } from "data-async-iterators";
import { fromStream } from 'data-async-iterators/lib/streams';
import { replay } from "data-async-iterators/lib/misc/replay";

export type ResetToken = 'R';
export var ResetToken : ResetToken = 'R';

export type LogEntryData<T> = [ number, TransactionState.Open, T ];
export type LogEntryCommit = [ number, TransactionState.Commited, number ];
export type LogEntryAbort = [ number, TransactionState.Aborted ];
export type LogEntryReset = ResetToken;

export type LogEntry<T> =
          LogEntryData<T>
        | LogEntryCommit
        | LogEntryAbort
        | LogEntryReset;

export function isLogEntryData<T> ( entry : LogEntry<T> ) : entry is LogEntryData<T> {
    return entry instanceof Array && entry.length == 3 && entry[ 1 ] == TransactionState.Open;
}

export function isLogEntryCommit ( entry : LogEntry<any> ) : entry is LogEntryCommit {
    return entry instanceof Array && entry.length == 3 && entry[ 1 ] == TransactionState.Commited;
}

export function isLogEntryAbort ( entry : LogEntry<any> ) : entry is LogEntryAbort {
    return entry instanceof Array && entry.length == 2 && entry[ 1 ] == TransactionState.Aborted;
}

export function isLogEntryReset ( entry : LogEntry<any> ) : entry is LogEntryReset {
    return typeof entry === 'string' && entry === ResetToken;
}

export interface StorageInterface<T> {
    reset () : Promise<void>;

    writeMany ( transaction : number, blocks : T[] ) : Promise<void>;
    
    commit ( transaction : number, commit : number ) : Promise<void>;

    abort ( transaction : number ) : Promise<void>;

    read ( afterCommit ?: number ) : AsyncIterable<LogEntry<T>>;

    close () : void;
}

export class FileStorage<T> implements StorageInterface<T> {
    protected file : string;

    protected keepInMemory : boolean;

    protected writer : number = null;

    protected writerLock : Semaphore = new Semaphore( 1 );

    protected readerBuffer : AsyncIterable<LogEntry<T>>;

    constructor ( file : string, keepInMemory : boolean = true ) {
        this.file = file;
        this.keepInMemory = keepInMemory;
    }

    async exists () : Promise<boolean> {
        return this.writer != null || await fs.exists( this.file );
    }

    protected async writeToFile ( entry : LogEntry<T>, sync : boolean = false ) : Promise<void> {
        if ( this.writer === null ) {
            this.writer = await fs.open( this.file, 'a+' );
        }

        await fs.appendFile( this.writer, JSON.stringify( entry ) + '\n', 'utf8' );

        if ( sync ) {
            await fs.fsync( this.writer );
        }
    }

    @SynchronizedBy( 'writerLock' )
    reset () : Promise<void> {
        return this.writeToFile( ResetToken, true );
    }

    @SynchronizedBy( 'writerLock' )
    async writeMany ( transaction : number, blocks : T[] ) : Promise<void> {
        for ( let block of blocks ) {
            await this.writeToFile( [ transaction, TransactionState.Open, block ] );
        }
    }

    @SynchronizedBy( 'writerLock' )
    commit ( transaction : number, commit : number ) : Promise<void> {
        return this.writeToFile( [ transaction, TransactionState.Commited, commit ], true );
    }

    @SynchronizedBy( 'writerLock' )
    abort ( transaction : number ) : Promise<void> {
        return this.writeToFile( [ transaction, TransactionState.Aborted ], true );
    }

    protected async * readLines () : AsyncIterableIterator<string> {
        if ( !await this.exists() ) return;

        const stream = fs.createReadStream( this.file, 'utf8' );

        let memory : string = '';

        for await ( let chunk of fromStream<string>( stream ) ) {
            const lines = chunk.split( '\n' );

            if ( lines.length == 1 ) {
                memory += lines[ 0 ];
            } else {
                for ( let line of lines ) {
                    if ( memory != '' ) {
                        yield memory + line;
                        memory = '';
                    } else {
                        yield line;
                    }
                }
            }
        }

        if ( memory != '' ) yield memory;
    }

    protected readInternal () : AsyncIterable<LogEntry<T>> {
        return dropErrors( map( this.readLines(), line => JSON.parse( line ) as LogEntry<T> ) );
    }

    read ( afterCommit ?: number ) : AsyncIterable<LogEntry<T>> {
        let iterable : AsyncIterable<LogEntry<T>>;

        if ( this.readerBuffer != null ) {
            iterable = this.readerBuffer;
        } else {
            iterable = dynamic( () => this.readInternal() );

            if ( this.keepInMemory ) {
                this.readerBuffer = replay( iterable, Infinity );
            }
        }

        // TODO
        // if ( typeof afterCommit != null ) {
        //     iterable = dropUntil( iterable, entry => isLogEntryCommit( entry ) && entry[ 2 ] >= afterCommit );
        // }

        return iterable;
    }
    
    @SynchronizedBy( 'writerLock' )
    async close () : Promise<void> {
        if ( this.writer != null ) {
            await fs.close( this.writer );

            this.writer = null;
        }
    }
}

export class SegmentedFileStorage<T> implements StorageInterface<T> {
    protected folder : string;

    protected name : string;

    protected file : FileStorage<T> = null;

    protected fileIndex : number = 0;

    protected fileEntries : number = 0;

    protected entriesPerFile : number;

    protected keepInMemory : boolean;

    protected fileLock : Semaphore = new Semaphore( 1 );

    constructor ( folder : string, name : string, entriesPerFile : number = 100, keepInMemory : boolean = true ) {
        this.folder = folder;
        this.name = name;
        this.entriesPerFile = entriesPerFile;
        this.keepInMemory = keepInMemory;
    }

    protected async getSegments () : Promise<number[]> {
        let files = await fs.readdir( this.folder );

        const pattern = new RegExp( `${this.name}-([0-9]+)\.jsonl` );

        return files.map( file => file.match( pattern ) )
            .filter( match => !!match )
            .map( match => +match[ 1 ] );
    }

    protected async getNextSegment () : Promise<number> {
        const max = ( await this.getSegments() ).reduce( ( a, b ) => Math.max( a, b ), 0 );

        const storage = this.openFile( max );

        try {
            if ( await storage.exists() ) {
                if ( await count( storage.read() ) >= this.entriesPerFile ) {
                    return max + 1;
                }
            }
        } finally {
            await storage.close();
        }

        return max;
    }

    protected openFile ( fileIndex : number = null ) : FileStorage<T> {
        return new FileStorage<T>( path.join( this.folder, `${ this.name }-${ fileIndex }.jsonl` ), this.keepInMemory );
    }

    protected async openWriter () {
        if ( this.file == null ) {
            const index = await this.getNextSegment();

            this.file = await this.openFile( index );

            if ( await this.file.exists() ) {
                this.fileEntries = await count( this.file.read() );
            }
        }
    }

    protected async flipWriter () {
        if ( this.file != null ) {
            await this.file.close();
        }

        this.file = this.openFile( ++this.fileIndex );

        this.fileEntries = 0;
    }

    @SynchronizedBy( 'fileLock' )
    async reset () : Promise<void> {
        await this.openWriter();

        await this.file.reset();

        if ( this.fileEntries >= this.entriesPerFile ) {
            await this.flipWriter();
        }
    }

    @SynchronizedBy( 'fileLock' )
    async writeMany ( transaction : number, blocks : T[] ) : Promise<void> {
        await this.openWriter();

        if ( this.fileEntries + blocks.length <= this.entriesPerFile ) {
            await this.file.writeMany( transaction, blocks );
            
            this.fileEntries++;
        } else {
            let remaining = blocks.length;

            while ( remaining > 0 ) {
                const before = blocks.slice( 0, this.entriesPerFile - this.fileEntries );

                blocks = blocks.slice( before.length );

                await this.file.writeMany( transaction, before );

                this.fileEntries += before.length;

                remaining -= before.length;

                if ( blocks.length > 0 ) {
                    await this.flipWriter();
                }
            }
        }

        if ( this.fileEntries >= this.entriesPerFile ) {
            await this.flipWriter();
        }
    }

    @SynchronizedBy( 'fileLock' )
    async commit ( transaction : number, commit : number ) : Promise<void> {
        await this.openWriter();

        await this.file.commit( transaction, commit );

        if ( this.fileEntries >= this.entriesPerFile ) {
            await this.flipWriter();
        }
    }

    @SynchronizedBy( 'fileLock' )
    async abort ( transaction : number ) : Promise<void> {
        await this.openWriter();

        await this.file.abort( transaction );

        this.fileEntries += 1;

        if ( this.fileEntries >= this.entriesPerFile ) {
            await this.flipWriter();
        }
    }

    read ( afterCommit ?: number ) : AsyncIterable<LogEntry<T>> {
        let lastFile : FileStorage<T> = null;

        const stream = AsyncStream.from( this.getSegments() );

        return stream.sort().map( async number => {
            if ( lastFile != null ) {
                await lastFile.close();
            }

            return lastFile = this.openFile( number );
        } ).flatMap( file => file.read( afterCommit ) );
    }

    @SynchronizedBy( 'fileLock' )
    close () : void {
        if ( this.file != null ) {
            this.file = null;

            this.fileIndex = 0;

            this.fileEntries = 0;
        }
    }
}