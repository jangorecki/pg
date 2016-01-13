options("digits.secs" = 3L)
options("pg.conn" = pgConnect(dbname = "r_db", user = "r_user", password = "r_password"))
meta = function(run_id = get_run_id(),
                r_node = as.character(Sys.info()[["nodename"]])[1L],
                r_timestamp = Sys.time(),
                r_fun = NA_character_,
                r_args = NA_character_) list(run_id=run_id, r_node=r_node, r_timestamp=r_timestamp, r_fun=r_fun, r_args=paste(r_args, collapse=","))
options("logR.conn" = getOption("pg.conn"),
        "logR.schema" = "r_tech",
        "logR.table" = "logr",
        "logR.meta" = meta)
