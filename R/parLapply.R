library(filehash)

pStack <- function(name) {
        ## A stack for the results
        rdbname <- paste(db$name, "result", sep = ".")
        list(db = createS(name),
             rdb = createS(rdbname))
}

plapply <- function(X, FUN, name = NULL) {
        ## Make "shared memory" stack
        if(is.null(name))
                name <- filehash:::sha1(X)
        p <- pStack(name)
        mpushS(p$db, X)

        ## Share the function via "shared memory"
        setFUN(p$db, FUN)
        
        repeat {
                while(inherits(obj <- try(popS(p$db)), "try-error"))
                        next
                if(is.null(obj))
                        break
                result <- FUN(obj)

                while(inherits(try(pushS(p$rdb, result)), "try-error"))
                        next
                ## Sys.sleep(0.5)
        }
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
                while(inherits(obj <- try(popS(db)), "try-error"))
                        next
                if(is.null(obj))
                        break
                result <- FUN(obj)

                while(inherits(try(pushS(rdb, result)), "try-error"))
                        next
                ## Sys.sleep(0.5)
        }
        invisible(NULL)
}

