schema_table = function(name){
    # transform schema.table into c(schema, table)
    stopifnot(is.character(name))
    if(length(name)==1L){
        if(length(grep(".", name, fixed = TRUE))) name = strsplit(name, split = ".", fixed = TRUE)[[1L]]
    } else if(length(name)==2L){
        stopifnot(!length(grep(".", name, fixed = TRUE)))
    }
    stopifnot(length(name)==2L)
    name
}

#' @title Create *run* table
#' @description *run* table generates run_id for `new_run_id` function from a sequence, it also stores R timestamp and db timestamp.
#' @param schema_name character scalar
#' @param table_name character scalar
#' @param drop logical not yet supported.
#' @param silent logical if TRUE will wrap into silent `try`.
#' @param conn active connection to postgres database, by default `getOption("pg.conn")`.
#' @param .log logical default `getOption("pg.log",TRUE)` decides if call is logged using *logR*.
#' @name create_run_table
create_run_table = function(schema_name, table_name = "run", drop = FALSE, silent = FALSE, conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(is.character(schema_name), length(schema_name)==1L, is.character(table_name), length(table_name)==1L, is.logical(drop), is.logical(silent))
    if(drop){
        stop("TO DO pgDropTable and reuse here")
    }
    sql = sprintf('CREATE TABLE %s.%s ("run_id" SERIAL NOT NULL PRIMARY KEY, "r_timestamp" TIMESTAMPTZ, "db_timestamp" TIMESTAMPTZ DEFAULT NOW());', schema_name, table_name)
    if(!silent){
        stopifnot(pgExistsSchema(schema_name, conn = conn, .log = .log), !pgExistsTable(c(schema_name,table_name), conn = conn, .log = .log))
        pgSendQuery(sql, conn = conn, .log = .log)
    } else {
        if(!pgExistsSchema(schema_name, conn = conn, .log = .log)) pgSendQuery(sprintf("CREATE SCHEMA %s;", schema_name), conn = conn, .log = .log)
        try(pgSendQuery(sql, conn = conn, .log = .log), silent = silent)
    }
    pgExistsTable(c(schema_name,table_name), conn = conn, .log = .log)
}

#' @title Generate new run id
#' @description Query data base for a new run id from sequence, storing also timestamp of the run id obtained.
#' @param conn active connection to postgres database, by default `getOption("pg.conn")`.
#' @param .log logical default `getOption("pg.log",TRUE)` decides if call is logged using *logR*.
#' @name new_run_id
new_run_id = function(conn = getOption("pg.conn"), .log = getOption("pg.log",TRUE)){
    stopifnot(!is.null(conn), is.logical(.log))
    run_id = pgGetQuery(sprintf("INSERT INTO r_tech.run (r_timestamp) VALUES ('%s+00'::TIMESTAMPTZ) RETURNING run_id;", format(Sys.time(), "%Y-%m-%d %H:%M:%OS")),
                        conn = conn,
                        .log = .log)$run_id
    run_id
}

#' @title Get current run id
#' @description This only access current `run_id`, in case of NULL it will result valid value `NA_integer_`.
#' @name get_run_id
get_run_id = function() as.integer(getOption("run_id"))[1L]

add_techstamp = function(x){
    stopifnot(is.data.table(x), length(x)>0L)
    x[, c("run_id","r_timestamp") := list(get_run_id(), Sys.time())]
}

rm_techstamp = function(x){
    stopifnot(is.data.table(x), length(x)>0L, c("run_id","r_timestamp") %in% names(x))
    x[, c("run_id","r_timestamp") := NULL][]
}

#' @title prettyprint.char
#' @description Wrapper to setup `options("datatable.prettyprint.char")` option. Affects data.table column width while printing.
#' @param trunc.char integer scalar passed as option value
#' @name ppc
ppc = function(trunc.char) options(datatable.prettyprint.char=trunc.char)
