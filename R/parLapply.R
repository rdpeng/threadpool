
#' @importFrom queue create_Q
#' @export
#'
cluster_make <- function(path) {
        cl <- list(injob = create_Q(sprintf("%s.in.q", path)),
                   outjob = create_Q(sprintf("%s.out.q", path)),
                   path = path)
        cl
}

#' @importFrom queue init_Q
#' @export
#'
cluster_join <- function(path) {
        cl <- list(injob = init_Q(sprintf("%s.in.q", path)),
                   outjob = init_Q(sprintf("%s.out.q", path)),
                   path = path)
        cl
}

#' @export
#'
new_task <- function(data, func) {
        list(data = data, func = func)
}

#' @importFrom queue enqueue
#' @export
#'
cluster_add1 <- function(cl, task) {
        enqueue(cl$injob, task)
}

#' @importFrom queue dequeue
#'
cluster_next_task <- function(cl) {
        task <- dequeue(cl$injob)
        task
}


#' @export
#' @importFrom queue is_empty
#'
cluster_run <- function(cl) {
        while(!is_empty(cl$injob)) {
                task <- cluster_next_task(cl)
                task_run(task)
        }
        invisible(NULL)
}

task_run <- function(task) {
        result <- with(task, func(data))
        result
}

task_output <- function(result) {
        result$output
}

#' @importFrom queue enqueue
#' @export
#'

cluster_finish_task <- function(cl, out) {
        enqueue(cl$outjob, out)
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

