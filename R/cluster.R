
cluster_paths <- function(name) {
        list(injob = file.path(name, sprintf("%s.in.q", name)),
             outjob = file.path(name, sprintf("%s.out.q", name)),
             meta = file.path(name, sprintf("%s.meta.rds", name)))
}

#' Delete a Cluster
#'
#' Clean up cluster-related files from the filesystem
#'
#' @param name cluster name
#' @export
#'
delete_cluster <- function(name) {
        p <- cluster_paths(name)
        cat("removing input queue...")
        unlink(p$injob, recursive = TRUE)
        cat("done!\n")
        cat("removing output queue...")
        unlink(p$outjob, recursive = TRUE)
        cat("done!\n")
        cat("removing metadata...")
        if(file.remove(p$meta))
                cat("done!\n")
        else {
                cat("\n")
                warning("problem removing metadata")
        }
        file.remove(name)
        invisible()
}

#' Create a Cluster
#'
#' Create the input job queue, the output job queue, and the metadata path
#' for a cluster
#'
#' @param name the name of the cluster
#' @param mapsize \code{mapsize} argument for underlying LMDB database
#'
#' @importFrom queue create_queue
#' @export
#'
cluster_create <- function(name, mapsize = getOption("threadpool_default_mapsize")) {
        p <- cluster_paths(name)
        cl <- list(injob = create_queue(p$injob, mapsize = mapsize),
                     outjob = create_queue(p$outjob, mapsize = mapsize),
                     meta = p$meta,
                     name = name)
        cl
}

#' Join a Cluster
#'
#' Join a currently running cluster in order to execute jobs
#'
#' @param name name of the cluster
#' @param mapsize \code{mapsize} argument for underlying LMDB database
#'
#' @description Given a cluster name, join that cluster and return a cluster
#' object for subsequent passing to \code{cluster_run}.
#'
#' @return A cluster object is returned.
#'
#' @importFrom queue init_queue
#' @export
#'
cluster_join <- function(name, mapsize = getOption("threadpool_default_mapsize")) {
        p <- cluster_paths(name)
        cl <- list(injob = init_queue(p$injob, mapsize = mapsize),
                   outjob = init_queue(p$outjob, mapsize = mapsize),
                   meta = p$meta,
                   name = name)
        cl
}

new_task <- function(data, func) {
        list(data = data, func = func)
}

#' Add One Task to a Cluster
#'
#' Add a task to the input queue of a cluster
#'
#' @param cl a cluster object
#' @param task a task object
#'
#' @importFrom queue enqueue
#' @export
#'
cluster_add1_task <- function(cl, task) {
        injob_q <- cl$injob
        enqueue(injob_q, task)
}

#' Retrieve the Next Task
#'
#' Retrieve the next task in the input queue for a cluster
#'
#' @param cl a cluster object
#'
#' @return a task object
#' @importFrom queue dequeue
#' @export
#'
cluster_next_task <- function(cl) {
        injob_q <- cl$injob
        task <- try(dequeue(injob_q), silent = TRUE)
        task
}


#' Run Tasks in a Cluster
#'
#' Begin running tasks from a cluster queue
#'
#' @param cl cluster object
#'
#' @description This function takes information about a cluster and begins
#' reading and executing tasks from the associated input queue.
#'
#' @return Nothing is returned.
#'
#' @export
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

#' Finish a Cluster Task
#'
#' Take the output from running a task and add it to the output queue
#'
#' @param cl a cluster object
#' @param out the output from a task
#'
#' @importFrom queue enqueue
#' @export
#'

cluster_finish_task <- function(cl, out) {
        enqueue(cl$outjob, out)
}


