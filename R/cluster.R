
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

#' Delete a Cluster
#'
#' Clean up cluster-related files from the filesystem
#'
#' @param name cluster name
#' @export
#' @importFrom queue delete_queue
#'
delete_cluster <- function(name) {
        cl <- cluster_join(name)
        delete_queue(cl$jobqueue)
        val <- unlink(name, recursive = TRUE)
        if(val > 0)
                warning(sprintf("problem deleting cluster '%s'", name))
        invisible(val)
}

#' Create a Cluster
#'
#' Create the input job queue, the output job queue, and other cluster elements
#'
#' @param name the name of the cluster
#'
#' @importFrom queue create_job_queue
#' @export
#'
cluster_create <- function(name) {
        dir.create(name)
        p <- cluster_paths(name)
        mapsize <- getOption("threadpool_default_mapsize") ## Needed for LMDB
        list(jobqueue = create_job_queue(p$jobqueue, mapsize = mapsize),
             logfile = p$logfile,
             env = p$env,
             name = name)
}

#' Join a Cluster
#'
#' Join a currently running cluster in order to execute jobs
#'
#' @param name name of the cluster
#'
#' @description Given a cluster name, join that cluster and return a cluster
#' object for subsequent passing to \code{cluster_run}.
#'
#' @return A cluster object is returned.
#'
#' @importFrom queue init_job_queue
#' @export
#'
cluster_join <- function(name) {
        if(!file.exists(name))
                stop(sprintf("cluster '%s' does not exist", name))
        p <- cluster_paths(name)
        mapsize = getOption("threadpool_default_mapsize")  ## Needed for LMDB
        cl <- list(jobqueue = init_job_queue(p$jobqueue, mapsize = mapsize),
                   logfile = p$logfile,
                   env = p$env,
                   name = name)
        cl
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
        job_q <- cl$jobqueue
        enqueue(job_q, task)
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
#' @export
#'
cluster_next_task <- function(cl) {
        job_q <- cl$jobqueue
        job_task <- try({
                input2shelf(job_q)
        }, silent = TRUE)
        job_task
}

#' Run Tasks in a Cluster
#'
#' Begin running tasks from a cluster queue
#'
#' @param cl cluster object
#' @param verbose print diagnostic messages?
#'
#' @description This function takes information about a cluster and begins
#' reading and executing tasks from the associated input queue.
#'
#' @return Nothing is returned.
#'
#' @importFrom utils capture.output
#' @export
#'
cluster_run <- function(cl, verbose = TRUE) {
        envir <- list2env(readRDS(cl$env))

        if(verbose) {
                pid <- Sys.getpid()
                cat("Starting cluster node:", pid, "\n")
        }
        while(!inherits(job_task <- cluster_next_task(cl), "try-error")) {
                task <- job_task$value
                result <- try({
                        msg <- capture.output({
                                taskout <- task_run(task, envir)
                        })
                        if(length(msg) > 0) {
                                message_log(cl, msg)
                        }
                        taskout
                })
                cluster_finish_task(cl, job_task, result)
        }
        invisible(NULL)
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
#' @export
#'

cluster_finish_task <- function(cl, job_task, output) {
        job_q <- cl$jobqueue
        shelf2output(job_q, job_task$key, output)
}


#' Read Results
#'
#' Read the results of a cluster run from the output queue
#'
#' @param cl cluster object
#'
#' @return a list with the results of the cluster output
#'
#' @importFrom digest digest
#' @importFrom queue dequeue
#' @export
#'
cluster_reduce <- function(cl) {
        job_q <- cl$jobqueue
        env <- new.env(size = 10000L)
        while(!inherits(try(out <- dequeue(job_q), silent = TRUE),
                        "try-error")) {
                key <- digest(out)
                env[[key]] <- out
        }
        keys <- ls(env, all.names = TRUE)
        results <- mget(keys, env)
        names(results) <- NULL
        results
}


#' Check for Abandoned Tasks
#'
#' Check the shelf for any abandoned tasks
#'
#' @param cl a cluster object
#'
#' @importFrom queue any_shelf
#'
#' @export
#'
abandoned <- function(cl) {
        any_shelf(cl$jobqueue)
}

#' Re-queue Abandoned Tasks
#'
#' Place any abandoned tasks on the shelf in the input queue
#'
#' @param cl a cluster object
#'
#' @importFrom queue shelf2input
#' @export
#'
requeue_abandoned <- function(cl) {
        if(!abandoned(cl))
                stop("there are no abandoned tasks")
        job_q <- cl$jobqueue
        shelf2input(job_q)
}












