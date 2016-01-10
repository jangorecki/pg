#' @name pg-package
#' @title pg - postgres utilities for R
#' @description Set of handy function when extensively working with postgres database. Includes transactional logging using *logR* package. Read `?pg` manual, use similarly to DBI functions.
#' @docType package
NULL

#' @name pg
#' @title pg* wrappers to DBI::db*
#' @description Many wrappers around `DBI::db*` functions. Some new wrappers added are `pgTruncateTable`, `pgExistsSchema`, `pgDropSchema`, `pgListTableColumns`. Follow `tests/tests.R` script for reproducible workflow with tests.
#' @param conn active connection to postgres database, by default `getOption("pg.conn")`.
#' @param .log logical default `getOption("pg.log",TRUE)` decides if call is logged using *logR*.
#' @param statement character scalar
#' @param name character
#' @param value data.table
#' @param key character vector of columns to be used to set data.table key on the db results.
#' @param norows arbitrat object which will be returned in case of 0 rows result from db.
#' @param stage_name character, staging schema-table name used for performing *Upsert*.
#' @param conflict_by character vector, will be used collapsed in `ON CONFLICT (conflict_by) DO ...`.
#' @param on_conflict character scalar to be send as `ON CONFLICT` postgres content. Key column set for conflict may be included here, but then should not be provided to `conflict_by` argument. Default `DO NOTHING`.
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
pgSendQuery = function(statement, silent = FALSE, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.character(statement), is.logical(.log))
    invisible(logR(dbSendQuery(conn, statement),
                   silent = silent,
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
    schema_name %in% schema$schema_name
}

#' @rdname pg
pgListTables = function(schema_name, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.logical(.log))
    if(!missing(schema_name)){
        in_schema = sprintf("and schemaname IN (%s)",paste(paste0("'",schema_name,"'"),collapse=","))
        logR(dbListTables(conn, in_schema),
             silent = FALSE,
             meta = meta(r_fun = "dbListTables", r_args = paste(schema_name, collapse=",")),
             .log = .log)
    } else {
        logR(dbListTables(conn),
             silent = FALSE,
             meta = meta(r_fun = "dbListTables"),
             .log = .log)
    }
}

#' @rdname pg
pgTruncateTable = function(name, silent = FALSE, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.logical(.log), is.logical(silent))
    # TO DO support vectorized
    name = schema_table(name)
    pgSendQuery(sprintf("TRUNCATE TABLE %s;", paste(name, collapse=".")),
                silent = silent,
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
    invisible(sapply(
        setNames(nm = schema_name),
        function(schema) pgSendQuery(sprintf("DROP SCHEMA %s%s;", schema, if(cascade) " CASCADE" else ""), silent = silent, conn = conn, .log = .log)
    ))
}

#' @rdname pg
pgRemoveTable = function(name, cascade = FALSE, silent = FALSE, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.logical(.log), is.character(name), is.logical(cascade), is.logical(silent))
    name = schema_table(name)
    # local dbRemoveTable to allow cascade
    dbRemoveTable = function(conn, name, cascade = FALSE){
        # https://github.com/tomoakin/RPostgreSQL/blob/35f4d7e4510992bee9b06a886eedac21f8688ebf/RPostgreSQL/R/PostgreSQL.R#L191
        if(RPostgreSQL::dbExistsTable(conn, name)){
            rc <- try(RPostgreSQL::dbGetQuery(conn, sprintf("DROP TABLE %s%s;", RPostgreSQL::postgresqlTableRef(name), if(cascade) " CASCADE" else "")))
            !inherits(rc, RPostgreSQL:::ErrorClass)
        }
        else FALSE
    }
    logR(dbRemoveTable(conn, name, cascade),
         silent = silent,
         meta = meta(r_fun = "dbRemoveTable", r_args = paste(name, collapse=".")),
         .log = .log)
}

#' @rdname pg
pgDropTable = function(name, cascade = FALSE, silent = FALSE, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.logical(.log), is.character(name), is.logical(cascade), is.logical(silent))
    # detect if c(schema,table), otherwise process already collapsed schema.table vectorized, similarly to pgDropSchema but here tricky detection is required
    vec = FALSE # TO DO
    if(!vec) name = setNames(list(name), paste(name, collapse="."))
    sapply(name, pgRemoveTable, cascade = cascade, silent = silent, conn = conn, .log = .log)
}

#' @rdname pg
pgSendUpsert = function(stage_name, name, conflict_by, on_conflict = "DO NOTHING", techstamp = TRUE, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.logical(.log), is.logical(techstamp), is.character(on_conflict), length(on_conflict)==1L)
    cols = pgListFields(stage_name)
    cols = setdiff(cols, c("run_id","r_timestamp")) # remove techstamp to have clean column list, as the fresh one will be used, if any
    # sql
    insert_into = sprintf("INSERT INTO %s.%s (%s)", name[1L], name[2L], paste(if(techstamp) c(cols, c("run_id","r_timestamp")) else cols, collapse=", "))
    select = sprintf("SELECT %s", paste(cols, collapse=", "))
    if(techstamp) select = sprintf("%s, %s::INTEGER run_id, '%s'::TIMESTAMPTZ r_timestamp", select, get_run_id(), format(Sys.time(), "%Y-%m-%d %H:%M:%OS"))
    from = sprintf("FROM %s.%s", stage_name[1L], stage_name[2L])
    if(!missing(conflict_by)) on_conflict = paste(paste0("(",paste(conflict_by, collapse=", "),")"), on_conflict)
    on_conflict = paste("ON CONFLICT",on_conflict)
    sql = paste0(paste(insert_into, select, from, on_conflict), ";")
    pgSendQuery(sql, conn = conn, .log = .log)
}

#' @rdname pg
pgUpsertTable = function(name, value, conflict_by, on_conflict = "DO NOTHING", stage_name, techstamp = TRUE, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.logical(.log), is.logical(techstamp), is.character(on_conflict), length(on_conflict)==1L)
    name = schema_table(name)
    if(!missing(stage_name)){
        stage_name = schema_table(stage_name)
        drop_stage = FALSE
    } else {
        stage_name = name
        stage_name[2L] = paste("tmp", stage_name[2L], sep="_")
        drop_stage = TRUE
    }
    if(pgExistsTable(stage_name)) pgTruncateTable(name = stage_name, conn = conn, .log = .log)
    pgWriteTable(name = stage_name, value = value, techstamp = techstamp, conn = conn, .log = .log)
    on.exit(if(drop_stage) pgDropTable(stage_name, conn = conn, .log = .log))
    pgSendUpsert(stage_name = stage_name, name = name, conflict_by = conflict_by, on_conflict = on_conflict, techstamp = techstamp, conn = conn, .log = .log)
}
