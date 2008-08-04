library(filehash)

plapply <- function(X, FUN, name = NULL) {
        ## Make shared "memory" stack
        if(is.null(name))
                name <- filehash:::sha1(X)
        dbl <- createS(name)
        putS(dbl, X)

        ## A stack for the data 'X'
        rdbname <- paste(dbl$name, "result", sep = ".")
        rdb <- createS(rdbname)

        ## Share the function 'FUN'
        con <- file(paste(dbl$name, "FUN", sep = "."), "wb")
        serialize(FUN, con)
        close(con)
        
        repeat {
                while(inherits(obj <- try(popS(dbl)), "try-error"))
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
        dbl <- initS(name)
        rdbname <- paste(dbl$name, "result", sep = ".")
        rdb <- initS(rdbname)

        con <- file(paste(dbl$name, "FUN", sep = "."), "rb")
        FUN <- unserialize(con)
        close(con)
        
        repeat {
                while(inherits(obj <- try(popS(dbl)), "try-error"))
                        next
                if(is.null(obj))
                        break
                result <- FUN(obj)

                while(inherits(try(putS(rdb, result)), "try-error"))
                        next
                Sys.sleep(0.5)
        }
}
