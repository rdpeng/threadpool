library(filehash)

plapply <- function(X, FUN, ...) {
        dbl <- makeQ(X)
        print(dbl$name)
        putQ(dbl, X)
        
        repeat {
                while(inherits(obj <- try(popQ(dbl)), "try-error"))
                        next
                if(is.null(obj))
                        break
                FUN(obj, ...)
                Sys.sleep(0.5)
        }
}

plapply2 <- function(name, FUN, ...) {
        dbl <- initQ(name)
        
        repeat {
                while(inherits(obj <- try(popQ(dbl)), "try-error"))
                        next
                if(is.null(obj))
                        break
                FUN(obj, ...)
                Sys.sleep(0.5)
        }
}

makeQ <- function(x) {
        name <- filehash:::sha1(x)
        createQ(name)
}
