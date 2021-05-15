# PostgreSQL Experiments

This repository demonstrates how a transactional database can be used to control the division of work between two threads (assuming that the workload processing state is represented in the database).
A transactional database can be used to enforce that either:

1. All work is done by a single thread
1. Work is divided evenly between multiple threads.

## All work is done by a single thread

This is done by locking all the workload rows and updating their status to done once they have been processed. 
Only one thread will be able to acquire the locked rows and by the time the second thread acquires the lock, the status of the workload rows will be set to DONE so there will be no work for the thread to do.

The database queries that makes this possible are:

```sql
BEGIN TRANSACTION
SELECT id,user_json FROM source WHERE is_processed = FALSE ORDER BY id FOR UPDATE;
UPDATE source SET is_processed = TRUE WHERE id = ?
COMMIT
```

- `BEGIN TRANSACTION` begins a transaction
- `SELECT .. WHERE is_processed = FALSE .. FOR UPDATE` selects the workload rows and locks them until the end of the transaction
- `UPDATE SET is_processed = TRUE` updates the status of the workload rows so that the second thread does not repeat the work.
- `COMMIT` commits the transaction and releases the lock.

## Work is divided evenly between multiple threads.

This is achieved using the following queries:

```
BEGIN TRANSACTION
SELECT user_json FROM source WHERE is_processed = FALSE ORDER BY id FOR UPDATE SKIP LOCKED LIMIT ?
COMMIT
```

- `BEGIN TRANSACTION` begins a transaction
- `SELECT .. ORDER BY .. WHERE is_processed = FALSE FOR UPDATE SKIP LOCKED LIMIT ..` - 
    
    Each part of this query has a role:
    
    `ORDER BY` is important because it ensures all threads have view of the workload rows in a consistent order. 
  
    `FOR UPDATE .. LIMIT ` locks a limited amount of the selected rows. The first thread will work on these rows.

    `SKIP LOCKED` instructs the database to ignore locked rows. This allows a second thread to pick up rows that aren't being worked on by another thread.

    `WHERE is_processed = FALSE` is a second line of defense to make sure a second thread does not pick up work that was already done.

- `COMMIT` commits the transaction and releases the lock.

## Practical Applications

In this repository, we've used the simple example of using a transactional database to control the division of work between one or more threads, but the same queries can be used to divide work between one or more nodes in a kubernetes cluster.
