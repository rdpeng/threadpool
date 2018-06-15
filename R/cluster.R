
#' Return cluster paths
#'
#' The cluster 'name' ends up being a directory on the filesystem. Then, there
#' separate files for the queues and evaluation environment.
#'
#' @param name cluster name
cluster_paths <- function(name) {
        list(jobqueue = file.path(name, sprintf("%s.q", basename(name))),
             logfile = logfile_create(name),
             env = file.path(name, sprintf("%s.env.rds", basename(name))))
}

#' Create a Cluster
#'
#' Create the input job queue, the output job queue, and other cluster elements
#'
#' @param name the name of the cluster
#'
#' @importFrom queue create_job_queue
#'
cluster_create <- function(name) {
        dir.create(name)
        path <- cluster_paths(name)
        mapsize <- getOption("threadpool_default_mapsize") ## Needed for LMDB
        list(jobqueue = create_job_queue(path$jobqueue, mapsize = mapsize),
             logfile = path$logfile,
             env = path$env,
             name = name)
}

new_task <- function(data, func) {
        list(data = data, func = func)
}


#' Retrieve the Next Task
#'
#' Retrieve the next task in the input queue for a cluster
#'
#' @param cl a cluster object
#'
#' @return a task object
#' @importFrom queue input2shelf
#'
cluster_next_task <- function(cl) {
        job_q <- cl$jobqueue
        job_task <- try({
                input2shelf(job_q)
        }, silent = TRUE)
        job_task
}


message_log <- function(cl, msg) {
        cat(msg, file = cl$logfile, sep = "\n", append = TRUE)
}

task_run <- function(task, envir) {
        result <- with(task, {
                do.call(func, list(data), envir = envir)
        })
        result
}

#' Finish a Cluster Task
#'
#' Take the output from running a task and add it to the output queue
#'
#' @param cl a cluster object
#' @param job_task a job_task object from the shelf
#' @param output the output from a task
#'
#' @importFrom queue shelf2output
#'

cluster_finish_task <- function(cl, job_task, output) {
        job_q <- cl$jobqueue
        shelf2output(job_q, job_task$key, output)
        cl
}













