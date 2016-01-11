.onLoad <- function(libname, pkgname){

    # replaces potentially logR default value to same from function
    if(is.null(lm <- getOption("logR.meta")) || identical(lm, list())) options("logR.meta" = function(...) list())

}
