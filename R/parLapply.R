library(filehash)

pStack <- function(name) {
        ## A stack for the results
        rdbname <- paste(name, "result", sep = ".")
        list(db = createS(name),
             rdb = createS(rdbname))
}

plapply <- function(X, FUN, name = NULL) {
        ## Make "shared memory" stack
        if(is.null(name))
                name <- filehash:::sha1(X)
        p <- pStack(name)
        ## Share the function via "shared memory"
        setFUN(p$db, FUN)

        ## Send the data out
        mpushS(p$db, X)

        worker(name)

        ## Wait for other workers to finish
        while(!isEmptyS(p$db))
                Sys.sleep(0.5)
        getResults(name)
}

getResults <- function(name) {
        db <- dbInit(paste(name, "result", sep = "."))
        keys <- dbList(db)
        keys <- keys[keys != "top"]
        obj <- dbMultiFetch(db, keys)
        obj <- lapply(obj, "[[", "value")
        unname(obj)
}

setFUN <- function(db, FUN) {
        dbInsert(db$stack, "FUN", FUN)
}

getFUN <- function(db) {
        dbFetch(db$stack, "FUN")
}

################################################################################

pollWorker <- function(name) {
        repeat {
                try(worker(name), silent = TRUE)
                Sys.sleep(0.5)
        }
}

worker <- function(name) {
        db <- initS(name)
        rdbname <- paste(db$name, "result", sep = ".")
        rdb <- initS(rdbname)
        FUN <- getFUN(db)

        if(isEmptyS(db))
                return(invisible(NULL))
        repeat {
                while(inherits(obj <- try(popS(db), silent = TRUE),
                               "try-error")) {
                        Sys.sleep(0.1)
                }
                if(is.null(obj))
                        break
                result <- FUN(obj)

                while(inherits(try(pushS(rdb, result), silent = TRUE),
                               "try-error")) {
                        Sys.sleep(0.1)
                }
        }
        invisible(NULL)
}

