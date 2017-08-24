library(queue)


makeCluster <- function(name) {
        list(injob = create_Q(sprintf("%s.in.q", name)),
             outjob = create_Q(sprintf("%s.out.q", name)),
             name = name)
}

joinCluster <- function(name) {
        list(injob = init_Q(sprintf("%s.in.q", name)),
             outjob = init_Q(sprintf("%s.out.q", name)),
             name = name)
}        

job_add1 <- function(obj, cl) {
        enqueue(cl$injob, obj)
}

addJobs <- function(X, FUN, cl) {
        lapply(X, function(data) {
                obj <- list(data = data, f = FUN)
                job_add1(obj, cl)
        })
        invisible()
}

job_remove <- function(cl) {
        dequeue(cl$injob)
}

job_run_next <- function(cl) {
        job <- job_remove(cl)
        with(job, f(data))
}

runJobs <- function(cl) {
        while(TRUE) {
                out <- job_run_next(cl)
                job_finish(out, cl)
        }
}

job_finish <- function(out, cl) {
        enqueue(cl$outjob, out)
}

getout <- function(cl) {
        results <- list()
        
        while(TRUE) {
                out <- dequeue(cl$outjob)
                if(is.null(out))
                        break
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

