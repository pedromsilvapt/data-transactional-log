# Transactional Log

> Safely and reliable serialize and write data to a file in transactions, and read them back again

# Installation
```shell
npm install --save data-transactional-log
```

# Usage

```typescript
import { TransactionalLog } from 'data-transactional-log';

const log = new TransactionalLog<number>( 'actions.log' );

// Each log is composed of transactions. Each transaction is simply a list of values of the same time.
await log.writeTransaction( [ 1, 2, 3 ] );

// In many cases we might want to create transactions with only one item. For that we can use the shortcut method
await log.write( 1 );

// The methods above are fine when we have the transaction's items upfront, but sometimes we do not. For that we can explicitly create a transaction
const transaction = log.transaction();

try {
    await transaction.write( 1 );
    
    await transaction.writeMany( [ 2, 3 ] );

    await transaction.commit();
} catch {
    // It is usually good practices to wrap the transaction inside a try/catch to prevent transaction-leaks:
    // those can happen when a transaction is never commited neither aborted
    await transaction.abort();
}

// After having written to a log, we can retrieve those transactions
for await ( let transaction of transaction.readTransactions() ) {
    console.log( transaction.blocks, transaction.commitId );
    // Would print the following
    // [ 1, 2, 3 ] 1
    // [ 1 ] 2
    // [ 1, 2, 3 ] 3
0}

// Sometimes we might not necessarily care about the transactions themselves, and just need the actual data
for await ( let block of transaction.read() ) {
    console.log( block ); 
    // Would print the numbers, one in each line
    // 1 2 3 1 1 2 3
}

// For convenience, we might need the last transaction read. For that we have
console.log( transaction.lastTransactionRead );
console.log( transaction.lastTransactionIdRead );
```
