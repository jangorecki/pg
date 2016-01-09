## docker users should be able to setup local environment using following
# docker run -it --rm -p 5432 -e POSTGRES_USER=r_user -e POSTGRES_PASSWORD=r_password -e POSTGRES_DB=r_db --name pg postgres:9.5
# Sys.setenv(POSTGRES_HOST="172.17.0.2")

suppressPackageStartupMessages({
    library(data.table)
    library(logR)
    library(pg)
})

options("pg.conn" = pgConnect(dbname = "r_db", user = "r_user", password = "r_password"))

meta = function(run_id=get_run_id(), r_user = as.character(Sys.info()[["user"]])[1L], r_timestamp = Sys.time(), r_fun = NA_character_, r_args = NA_character_) list(run_id=run_id, r_user=r_user, r_timestamp=r_timestamp, r_fun=r_fun, r_args=paste(r_args, collapse=","))
create_meta = list(run_id = "INTEGER", r_user = "VARCHAR(255)", r_timestamp = "TIMESTAMPTZ", r_fun = "VARCHAR(255)", r_args = "VARCHAR(255)")
options("logR.conn" = getOption("pg.conn"),
        "logR.schema" = "r_tech",
        "logR.table" = "logr",
        "logR.meta" = meta)

# restart schemas
pgDropSchema(c("r_tech","r_data"), cascade = TRUE, silent = TRUE, .log = FALSE)
options("run_id" = NULL)
options("digits.secs" = 3L)
logR_schema(meta = create_meta)

# expected tables and column count in db
stopifnot(all.equal(pgListTableColumns(.log=FALSE)[, .N, .(table_schema, table_name)], data.table(table_schema="r_tech", table_name="logr", N=18L, key = c("table_schema","table_name"))))

# expected row count and status content with and without `.log` argument, valid only for sequence run after rebuilding schema
stopifnot(identical(pgGetQuery("SELECT * FROM r_tech.logr;")$status, NA_character_))
stopifnot(identical(pgGetQuery("SELECT * FROM r_tech.logr;", .log = FALSE)$status, "success"))

# create `run` table, validate created count of column
create_run_table(schema_name = "r_tech", table_name = "run", .log = FALSE)
stopifnot(all.equal(pgListTableColumns(.log=FALSE)[, .N, .(table_schema, table_name)], data.table(table_schema=rep("r_tech",2L), table_name=c("logr","run"), N=c(18L,3L), key = c("table_schema","table_name"))))

# create schema data
pgSendQuery("CREATE SCHEMA r_data;")

# validate 2 currently written logs
stopifnot(all.equal(pgGetQuery("SELECT * FROM r_tech.logr;", .log = FALSE)[, .N, .(r_fun, status)], data.table(r_fun=c("dbGetQuery","dbSendQuery"), status=rep("success",2L), N=c(1L,1L))))

# validate techstamp and nrows
dt = data.table(a = 1:5, b = letters[5:1])
pgWriteTable(c("r_data","non_techstamp"), dt, techstamp = FALSE)
pgWriteTable(c("r_data","techstamp"), dt, techstamp = TRUE)
stopifnot(identical(dim(pgReadTable(c("r_data","techstamp"))), 5:4), identical(dim(pgReadTable(c("r_data","non_techstamp"))), c(5L,2L)))

# validate truncate
pgTruncateTable(c("r_data","non_techstamp"))
stopifnot(pgGetQuery("SELECT count(*) cnt FROM r_data.non_techstamp;")$cnt==0L)

# validate run_id batches
pgTruncateTable(c("r_data","techstamp"))
dt = data.table(a = 1:2, b = letters[2:1])
pgWriteTable(c("r_data","techstamp"), dt) # options("run_id" = NULL) # should be already with this value while running test
options("run_id" = new_run_id(.log = FALSE))
dt = data.table(a = 4:6, b = letters[6:4])
pgWriteTable(c("r_data","techstamp"), dt)
options("run_id" = new_run_id(.log = FALSE))
dt = data.table(a = 5:8, b = letters[8:5])
pgWriteTable(c("r_data","techstamp"), dt)

# validate number of rows per batch
stopifnot(all.equal(pgReadTable(c("r_data","techstamp"), .log = FALSE)[, .N, run_id], data.table(run_id = c(NA, 1L, 2L), N = 2:4)))

# validate logs on writing batches
stopifnot(all.equal(pgReadTable(c("r_tech","logr"), .log = FALSE)[(length(logr_id)-2L):length(logr_id), .(in_rows, .N), run_id], data.table(run_id = c(NA, 1L, 2L), in_rows = 2:4, N = rep(1L,3L))))

# temporal query: latest
stopifnot(all.equal(
    pgGetQuery("SELECT DISTINCT ON (a) * FROM r_data.techstamp ORDER BY a, r_timestamp DESC;"),
    pgGetQuery("SELECT * FROM r_data.techstamp;")[order(-r_timestamp), head(.SD, 1L), .(a)][order(a)]
))

# temporal query: point-in-time


# temporal query: difference


q("no")
