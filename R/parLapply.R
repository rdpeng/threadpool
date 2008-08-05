library(filehash)

plapply <- function(X, FUN, name = NULL) {
        ## Make "shared memory" stack
        if(is.null(name))
                name <- filehash:::sha1(X)
        db <- createS(name)
        pushS(db, X)

        ## A stack for the results
        rdbname <- paste(db$name, "result", sep = ".")
        rdb <- createS(rdbname)

        ## Share the function via "shared memory"
        dbInsert(db, "FUN", FUN)
        
        repeat {
                while(inherits(obj <- try(popS(db)), "try-error"))
                        next
                if(is.null(obj))
                        break
                result <- FUN(obj)

                while(inherits(try(putS(rdb, result)), "try-error"))
                        next
                Sys.sleep(0.5)
        }
}

worker <- function(name) {
        db <- initS(name)
        rdbname <- paste(db$name, "result", sep = ".")
        rdb <- initS(rdbname)
        FUN <- dbFetch(db, "FUN")

        if(isEmptyS(db))
                return(invisible(NULL))
        repeat {
                while(inherits(obj <- try(popS(db)), "try-error"))
                        next
                if(is.null(obj))
                        break
                result <- FUN(obj)

                while(inherits(try(putS(rdb, result)), "try-error"))
                        next
                Sys.sleep(0.5)
        }
        invisible(NULL)
}

