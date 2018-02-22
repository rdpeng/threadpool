
cluster_paths <- function(path) {
        list(injob = sprintf("%s.in.q", path),
             outjob = sprintf("%s.out.q", path),
             meta = sprintf("%s.meta.rds", path))
}

#' @export
#'
delete_cluster <- function(path) {
        p <- cluster_paths(path)
        unlink(p$injob, recursive = TRUE)
        unlink(p$outjob, recursive = TRUE)
        file.remove(p$meta)
}

#' @importFrom queue create_queue
#' @export
#'
cluster_create <- function(path) {
        p <- cluster_paths(path)
        cl <- list(injob = create_queue(p$injob),
                     outjob = create_queue(p$outjob),
                     meta = p$meta,
                     path = path)
        cl
}

#' @importFrom queue init_queue
#' @export
#'
cluster_join <- function(path) {
        p <- cluster_paths(path)
        cl <- list(injob = init_queue(p$injob),
                   outjob = init_queue(p$outjob),
                   meta = p$meta,
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
cluster_add1_task <- function(cl, task) {
        injob_q <- cl$injob
        enqueue(injob_q, task)
}

#' @importFrom queue dequeue
#' @export
#'
cluster_next_task <- function(cl) {
        injob_q <- cl$injob
        task <- try(dequeue(injob_q), silent = TRUE)
        task
}


#' @export
#' @importFrom queue is_empty
#'
cluster_run <- function(cl) {
        meta <- readRDS(cl$meta)

        while(!inherits(task <- cluster_next_task(cl), "try-error")) {
                result <- task_run(task, meta)
                output <- task_output(result)
                cluster_finish_task(cl, output)
        }
        invisible(NULL)
}

task_run <- function(task, meta) {
        result <- with(task, do.call(func, list(data, meta)))
        ## result <- with(task, func(data, meta))
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


