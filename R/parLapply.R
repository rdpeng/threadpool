library(filehash)

plapply <- function(X, FUN, name = NULL) {
        ## Make shared "memory" queue
        if(is.null(name))
                name <- filehash:::sha1(X)
        dbl <- createQ(name)
        putQ(dbl, X)

        ## A queue for the data 'X'
        rdbname <- paste(dbl$name, "result", sep = ".")
        rdb <- createQ(rdbname)

        ## Share the function 'FUN'
        con <- file(paste(dbl$name, "FUN", sep = "."), "wb")
        serialize(FUN, con)
        close(con)
        
        repeat {
                while(inherits(obj <- try(popQ(dbl)), "try-error"))
                        next
                if(is.null(obj))
                        break
                result <- FUN(obj)

                while(inherits(try(putQ(rdb, result)), "try-error"))
                        next
                Sys.sleep(0.5)
        }
}

worker <- function(name) {
        dbl <- initQ(name)
        rdbname <- paste(dbl$name, "result", sep = ".")
        rdb <- initQ(rdbname)

        con <- file(paste(dbl$name, "FUN", sep = "."), "rb")
        FUN <- unserialize(con)
        close(con)
        
        repeat {
                while(inherits(obj <- try(popQ(dbl)), "try-error"))
                        next
                if(is.null(obj))
                        break
                result <- FUN(obj)

                while(inherits(try(putQ(rdb, result)), "try-error"))
                        next
                Sys.sleep(0.5)
        }
}
