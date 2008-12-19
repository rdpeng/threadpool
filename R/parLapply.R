library(filehash)


makeCluster <- function(name) {
        list(injob = createQ(sprintf("%s.in.q", name)),
             outjob = createQ(sprintf("%s.out.q", name)),
             name = name)
}

joinCluster <- function(name) {
        list(injob = initQ(sprintf("%s.in.q", name)),
             outjob = initQ(sprintf("%s.out.q", name)),
             name = name)
}        

pushjob <- function(obj, cl) {
        while(TRUE) {
                r <- tryCatch({
                        push(cl$injob, obj)
                }, error = function(e) {
                        e
                })
                if(!inherits(r, "condition"))
                        return(invisible())
                else
                        Sys.sleep(0.2)
        }
}

addJobs <- function(X, FUN, cl) {
        lapply(X, function(data) {
                obj <- list(data = data, f = FUN)
                pushjob(obj, cl)
        })
        invisible()
}

popjob <- function(cl) {
        while(TRUE) {
              job <- tryCatch({
                      pop(cl$injob)
              }, error = function(e) {
                      e
              })
              if(!inherits(job, "condition"))
                      return(job)
              else
                      Sys.sleep(0.2)
      }
}

runNextJob <- function(cl) {
        job <- popjob(cl)
        with(job, f(data))
}

runJobs <- function(cl) {
        while(TRUE) {
                out <- runNextJob(cl)
                finishJob(out, cl)
        }
}

finishJob <- function(out, cl) {
        while(TRUE) {
                r <- tryCatch({
                        push(cl$outjob, out)
                }, error = function(e) {
                        e
                })
                if(!inherits(r, "condition"))
                        return(invisible())
                else
                        Sys.sleep(0.2)
        }
}

getout <- function(cl) {
        results <- list()
        
        while(TRUE) {
                out <- tryCatch({
                        pop(cl$outjob)
                }, error = function(e) {
                        cm <- conditionMessage(e)

                        if(cm == "cannot create lock file")
                                e
                        else if(cm == "queue is empty")
                                NULL
                        else
                                e
                })
                if(is.null(out))
                        break
                else if(inherits(out, "condition"))
                        Sys.sleep(0.2)
                else
                        results <- c(results, out)
        }
        results
}

################################################################################

spinWorker <- function(name) {
        repeat {
                try(worker(name), silent = TRUE)
                Sys.sleep(0.5)
        }
}

worker <- function(name) {
        db <- initS(name)
        rdbname <- paste(db@name, "result", sep = ".")
        rdb <- initS(rdbname)
        FUN <- getFUN(db)

        repeat {
                empty <- try(isEmpty(db), silent = TRUE)

                if(inherits(empty, "try-error"))
                        next
                if(empty)
                        return(invisible(NULL))
                if(!inherits(obj <- try(pop(db), silent = TRUE), "try-error")) {
                        result <- FUN(obj)

                        while(inherits(try(push(rdb, result), silent = TRUE),
                                       "try-error")) {
                                Sys.sleep(0.1)
                        }
                }
                else
                        Sys.sleep(0.1)
        }
        invisible(NULL)
}

