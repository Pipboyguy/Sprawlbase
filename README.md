# SprawlHub

The sprawling network of community driven computation and storage.

This is the Postgres Server and Web Framework that manages scheduler/worker/DB interactions.

### Todo

- Functionality to update scheduler/client/worker information to PG
    - `Client.scheduler_info` gives relevant info
    - `Client.id` gives client ID
    - `Client.running` should give some indication on scheduler health

#### Tests

- [ ] Functional - SQL - `test_sql`
- [ ] Functional - Work 1 sprawlbuck - `test_one_sprawlbuck`
- [ ] Functional - Work done triggers transaction - ` test_work_triggers_transaction`

### In Progress

### Done ✓

- [x] Sprawlhub - `list_users`
- [x] Figure out how to identify each agent in a transaction
- [x] Design ERD for component interaction
- [x] Seed database with some data
- [x] Sprawlhub - can connect to Postgres as user `sprawlhub`
- [x] Implement `read_user_balance`
- [x] Sprawlhub - can read user balance - `test_read_user_balance`
- [x] Change SQL to ORM
- [x] Implement `register_user`
- [x] Implement `remove_user`
- [x] Sprawlhub - can register user - `register_user`
- [x] Sprawlhub - can remove user - `remove_user`
- [x] Sprawlhub - for tests, generate fixture that writes people to disk and read
- [x] Functional - Pandas dataframe process - `test_pandas`
- [x] `register_scheduler`
- [x] Unit - `test_register_scheduler`
