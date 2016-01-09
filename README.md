# pg

Postgres utilities for R language

- based on RPostgreSQL [RPostgreSQL](https://github.com/tomoakin/RPostgreSQL)
- logging powered by [logR](https://github.com/jangorecki/logR)

Features:  

- wrappers on many commonly used `DBI::db*` functions, enables logging, default `conn` and other arguments
- new functions: `pgExistsSchema`, `pgTruncateTable`, `pgListTableColumns`, `pgDropSchema`, `pgListTableColumns` and potentially more in future
- data transfered over `pgWriteTable` enables transparent technical metadata stamping
- metadata for logging with logR can store custom arbitrary information
- helpers for processing batches of the data

How to use:  

- install as R package: `install.packages("pg", repos = "soon")`, `devtools::install_github("jangorecki/pg")`
- follow workflow examples in [tests/tests.R](tests/tests.R)
- full environment can be easily reproduced from CI yaml file

Only postgres 9.5+ support is planned, currently there is no 9.5+ specific code yet.  
