import { AsyncStream, AsyncIterableLike, forEach, dropErrors } from 'data-async-iterators';
import { TransactionState } from './Core';
import { StorageInterface, isLogEntryData, isLogEntryAbort, isLogEntryReset, isLogEntryCommit, LogEntryCommit, FileStorage } from './Storages';

export interface TransactionInterface<T> {
    readonly id : number;

    blocks : T[];

    state : TransactionState;

    write ( block : T ) : Promise<void>;

    writeMany ( blocks : T[] ) : Promise<void>;

    commit () : Promise<void>;

    abort () : Promise<void>;
}

export interface TransactionalLogInterface<T> {
    lastTransactionRead : Transaction<T>;

    lastTransactionIdRead : number;

    readTransactions () : AsyncStream<Transaction<T>>;

    read () : AsyncStream<T>;

    reset () : Promise<void>;

    writeTransaction ( blocks : T[] ) : Promise<void>;

    write ( block : T ) : Promise<void>;

    transaction () : Promise<Transaction<T>>;
}

export class InvalidLogWrite extends Error { }

export class Transaction<T> implements TransactionInterface<T> {
    readonly id : number;

    readonly blocks : T[];

    state : TransactionState;

    protected log : TransactionalLogControllerInterface<T>;

    constructor ( id : number, log : TransactionalLogControllerInterface<T>, blocks : T[] = [], state : TransactionState = TransactionState.Open ) {
        this.id = id;
        this.log = log;
        this.blocks = blocks;
        this.state = state;
    }

    async write ( block : T ) : Promise<void> {
        if ( this.state != TransactionState.Open ) {
            throw new InvalidLogWrite( `Transaction is ${ this.state } and cannot be written to.` );
        }

        await this.log.getStorage().writeMany( this.id, [ block ] );

        this.blocks.push( block );
    }

    async writeMany ( blocks : T[] ) : Promise<void> {
        if ( this.state != TransactionState.Open ) {
            throw new InvalidLogWrite( `Transaction is ${ this.state } and cannot be written to.` );
        }
        
        await this.log.getStorage().writeMany( this.id, blocks );

        this.blocks.push( ...blocks );
    }

    async commit () : Promise<void> {
        if ( this.state != TransactionState.Open ) {
            throw new InvalidLogWrite( `Transaction is ${ this.state } and cannot be written to.` );
        }

        await this.log.getStorage().commit( this.id, await this.log.commit() );

        this.state = TransactionState.Commited;

        this.log = null;
    }

    async abort () : Promise<void> {
        if ( this.state != TransactionState.Open ) {
            throw new InvalidLogWrite( `Transaction is ${ this.state } and cannot be written to.` );
        }

        await this.log.getStorage().abort( this.id );

        this.state = TransactionState.Aborted;

        this.log = null;
    }
}

export interface TransactionalLogControllerInterface<T> {
    getStorage () : StorageInterface<T>;

    commit () : Promise<number>;
}

export class TransactionalLog<T> implements TransactionalLogInterface<T> {
    protected storage : StorageInterface<T>;

    protected transactionCounter = 0;

    protected commitCounter = 0;

    protected controller : TransactionalLogControllerInterface<T>;

    protected initLoad : Promise<void>;

    lastTransactionRead : Transaction<T> = null;

    lastTransactionIdRead : number = 0;

    constructor ( storage : StorageInterface<T> | string ) {
        if ( typeof storage === 'string' ) {
            storage = new FileStorage<T>( storage );
        }

        this.storage = storage;

        this.controller = {
            getStorage: () => this.storage,
            commit: () => this.commit()
        };

        this.initLoad = AsyncStream.from( this.storage.read() ).dropErrors().map<[number, number]>( entry => {
            if ( isLogEntryData( entry ) ) {
                return [ entry[ 0 ], null ];
            } else if ( isLogEntryAbort( entry ) ) {
                return [ entry[ 0 ], null ];
            } else if ( isLogEntryCommit( entry ) ) {
                return [ entry[ 0 ], entry[ 2 ] ];
            }
        } ).reduce( ( a, b ) => [ Math.max( a[ 0 ], b[ 0 ] ), Math.max( a[ 1 ], b[ 1 ] ) ], [ 0, 0 ] ).then( res => {
            this.transactionCounter = res[ 0 ];
            this.commitCounter = res[ 1 ];
        } );
    }

    reset () {
        return this.storage.reset();
    }

    readTransactions ( afterCommit : number = null ) : AsyncStream<Transaction<T>> {
        return AsyncStream.dynamic( () => {
            const transactions : Map<number, T[]> = new Map();

            const commited : Map<number, T[]> = new Map();

            return new AsyncStream( this.storage.read() ).tap( entry => {
                if ( isLogEntryData( entry ) ) {
                    const [ transaction, _, data ] = entry;

                    let blocks = transactions.get( transaction );

                    if ( !blocks ) {
                        transactions.set( transaction, blocks = [] );
                    }

                    blocks.push( data );
                } else if ( isLogEntryAbort( entry ) ) {
                    transactions.delete( entry[ 0 ] );
                } else if ( isLogEntryCommit( entry ) ) {
                    commited.set( entry[ 0 ], transactions.get( entry[ 0 ] ) );

                    transactions.delete( entry[ 0 ] );
                } else if ( isLogEntryReset( entry ) ) {
                    transactions.clear();
                }
            } ).filter( isLogEntryCommit ).map( ( entry : LogEntryCommit ) =>{
                const transaction = new Transaction( entry[ 2 ], this.controller, commited.get( entry[ 0 ] ), TransactionState.Commited );

                commited.delete( entry[ 0 ] );

                this.lastTransactionRead = transaction;

                this.lastTransactionIdRead = transaction.id;

                return transaction;
            } ).filter( transaction => transaction.id > afterCommit ).observe( { onEnd: () => {
                transactions.clear();
                commited.clear();
            } } );
        } );
    }

    read ( afterCommit : number = null ) : AsyncStream<T> {
        return this.readTransactions( afterCommit ).flatMap( transaction => transaction.blocks );
    }

    protected async commit () : Promise<number> {
        return ++this.commitCounter;
    }
    
    async transaction () : Promise<Transaction<T>> {
        // TODO Read the biggest transaction id
        await this.initLoad;

        return new Transaction<T>( ++this.transactionCounter, this.controller );
    }

    async writeTransaction ( blocks : AsyncIterableLike<T> ) : Promise<void> {
        const transaction = await this.transaction();

        try {
            await forEach( blocks, block => transaction.write( block ) );

            await transaction.commit();
        } catch ( error ) {
            await transaction.abort();

            throw error;
        }
    }

    async write ( block : T ) : Promise<void> {
        return this.writeTransaction( [ block ] );
    }
}
