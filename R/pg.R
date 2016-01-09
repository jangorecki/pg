#' @name pg-package
#' @title pg - postgres utilities for R
#' @description Set of handy function when extensively working with postgres database. Includes transactional logging using *logR* package. Read `?pg` manual, use similarly to DBI functions.
#' @docType package
NULL

#' @name pg
#' @title pg* wrappers to DBI::pg*
#' @description Many wrappers around `DBI::db*` functions. Some new wrappers added are `pgTruncateTable`, `pgExistsSchema`, `pgDropSchema`, `pgListTableColumns`. Follow `tests/tests.R` script for reproducible workflow with tests.
#' @param conn active connection to postgres database, by default `getOption("pg.conn")`.
#' @param .log logical default `getOption("pg.log",TRUE)` decides if call is logged using *logR*.
#' @param statement character scalar
#' @param name character
#' @param value data.table
#' @param key character vector of columns to be used to set data.table key on the db results.
#' @param norows arbitrat object which will be returned in case of 0 rows result from db.
#' @param techstamp logical decides if `dbWriteTable` will add technical metadata on saving each object to db.
#' @param schema_name character vector of schema names to be checked by `pgExistsSchema` or dropped by `pgDropSchema`.
#' @param select character vector of column names to fetch from `information_schema.columns` table.
#' @param cascade logical use cascade drop while dropping object.
#' @param silent logical catch potential errors, useful for potentially non existing ojbect on while dropping.
#' @param host character hostname/ip by default from ENV var `POSTGRES_HOST`
#' @param port character port by default from ENV var `POSTGRES_PORT`
#' @param dbname character port by default from ENV var `POSTGRES_DB`
#' @param user character port by default from ENV var `POSTGRES_USER`
#' @param password character port by default from ENV var `POSTGRES_PASSWORD`
#' @details
#' 1. Instead of reusing `conn` arg in each query just save it to option `options("pg.conn" = pgConnect())` and it will be reused.
#' 2. Logging is by default enabled, table for logs needs to be created, use `logR::logR_schema()` potentially with custom defined metadata columns. See `tests/tests.R` or *logR* package documentation.
#' 3. Stamping data by technical run id requires another table which can be created using `create_run_table()`. This feature has to be utilized as logR custom metadata values, see `tests/tests.R`.

#' @rdname pg
pgConnect = function(host = Sys.getenv("POSTGRES_HOST", "127.0.0.1"), port = Sys.getenv("POSTGRES_PORT", "5432"), dbname = Sys.getenv("POSTGRES_DB", "postgres"), user = Sys.getenv("POSTGRES_USER", "postgres"), password = Sys.getenv("POSTGRES_PASSWORD", "postgres")){
    dbConnect(PostgreSQL(), host = host, port = port, dbname = dbname, user = user, password = password)
}

#' @rdname pg
pgSendQuery = function(statement, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.character(statement), is.logical(.log))
    invisible(logR(dbSendQuery(conn, statement),
                   silent = FALSE,
                   meta = meta(r_fun = "dbSendQuery", r_args = statement),
                   .log = .log))
}

#' @rdname pg
pgGetQuery = function(statement, key, norows, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.character(statement), is.logical(.log))
    data = logR(dbGetQuery(conn, statement),
                silent = FALSE,
                meta = meta(r_fun = "dbGetQuery", r_args = statement),
                .log = .log)
    if(!missing(norows) && !nrow(data)) return(norows)
    if(!missing(key)) setDT(data, key = key)[] else setDT(data)[]
}

#' @rdname pg
pgWriteTable = function(name, value, techstamp = TRUE, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.logical(.log), is.logical(techstamp))
    name = schema_table(name)
    if(techstamp){
        add_techstamp(value) # by meta cols ref
        on.exit(rm_techstamp(value))
    }
    invisible(logR(dbWriteTable(conn, name, value, row.names = FALSE, append = TRUE, match.cols = TRUE), # match.cols requires 0.4.1 - fork of the CRAN's  RPostgreSQL
                   in_rows = nrow(value),
                   silent = FALSE,
                   meta = meta(r_fun = "dbWriteTable", r_args = paste(name, collapse=".")),
                   .log = .log))
}

#' @rdname pg
pgReadTable = function(name, key, norows, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.logical(.log))
    name = schema_table(name)
    data = logR(dbReadTable(conn, name),
                silent = FALSE,
                meta = meta(r_fun = "dbReadTable", r_args = paste(name, collapse=".")),
                .log = .log)
    if(!missing(norows) && !nrow(data)) return(norows)
    if(!missing(key)) setDT(data, key = key)[] else setDT(data)[]
}

#' @rdname pg
pgExistsTable = function(name, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.logical(.log))
    name = schema_table(name)
    logR(dbExistsTable(conn, name),
         silent = FALSE,
         meta = meta(r_fun = "dbExistsTable", r_args = paste(name, collapse=".")),
         .log = .log)
}

#' @rdname pg
pgExistsSchema = function(schema_name, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.logical(.log), is.character(schema_name), length(schema_name)>0L)
    schema = pgGetQuery(sprintf("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN (%s);", paste(paste0("'",schema_name,"'"), collapse=",")),
                        norows = data.table(schema_name = character(0)),
                        conn = conn,
                        .log = .log)
    if(length(schema_name)==1L) as.logical(nrow(schema)) else nrow(schema)==length(schema_name)
}

#' @rdname pg
pgListTables = function(conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.logical(.log))
    logR(dbListTables(conn),
         silent = FALSE,
         meta = meta(r_fun = "dbListTables"),
         .log = .log)
}

#' @rdname pg
pgTruncateTable = function(name, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.logical(.log))
    name = schema_table(name)
    pgSendQuery(sprintf("TRUNCATE TABLE %s;", paste(name, collapse=".")),
                conn = conn,
                .log = .log)
}

#' @rdname pg
pgListFields = function(name, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.logical(.log))
    name = schema_table(name)
    logR(dbListFields(conn, name),
         silent = FALSE,
         meta = meta(r_fun = "dbListFields"),
         conn = conn,
         .log = .log)
}

#' @rdname pg
pgListTableColumns = function(schema_name, select = c("table_schema", "table_name", "column_name", "ordinal_position", "column_default", "is_nullable", "data_type", "character_maximum_length", "numeric_precision", "datetime_precision"), conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.logical(.log), is.character(select), length(select) > 0L, c("table_schema","table_name","ordinal_position") %in% select)
    sql = sprintf("SELECT %s FROM information_schema.columns WHERE table_schema != 'pg_catalog' AND table_schema != 'information_schema' %sORDER BY table_schema, table_name, ordinal_position;",
                  paste(select, collapse=", "),
                  if(!missing(schema_name)){
                      stopifnot(is.character(schema_name), length(schema_name)>0L)
                      sprintf("AND table_schema IN (%s) ", paste(paste0("'",schema_name,"'"), collapse=", "))
                  } else "")
    DT = pgGetQuery(sql, conn = conn, .log = .log)
    if("is_nullable" %in% select) DT[, is_nullable := c("YES" = TRUE, "NO" = FALSE)[is_nullable]]
    setkeyv(DT, c("table_schema","table_name","ordinal_position"))[]
}

#' @rdname pg
pgDropSchema = function(schema_name, cascade = FALSE, silent = FALSE, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.logical(.log), is.character(schema_name), length(schema_name)>0L, is.logical(cascade), is.logical(silent))
    invisible(lapply(setNames(nm = schema_name),
                     function(schema){
                         sql = sprintf("DROP SCHEMA %s%s;", schema, if(cascade) " CASCADE" else "")
                         if(silent) try(pgSendQuery(sql, conn = conn, .log = .log), silent = silent)
                         else pgSendQuery(sql, conn = conn, .log = .log)
                     }))
}
