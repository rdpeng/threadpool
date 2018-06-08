
#' Return cluster paths
#'
#' The cluster 'name' ends up being a directory on the filesystem. Then, there
#' separate files for the queues and evaluation environment.
#'
#' @param name cluster name


cluster_paths <- function(name) {
        list(injob = file.path(name, sprintf("%s.in.q", basename(name))),
             outjob = file.path(name, sprintf("%s.out.q", basename(name))),
             logfile = logfile_create(name),
             env = file.path(name, sprintf("%s.env.rds", basename(name))))
}

#' Delete a Cluster
#'
#' Clean up cluster-related files from the filesystem
#'
#' @param name cluster name
#' @export
#'
delete_cluster <- function(name) {
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
#' @importFrom queue create_queue
#' @export
#'
cluster_create <- function(name) {
        dir.create(name)
        p <- cluster_paths(name)
        mapsize <- getOption("threadpool_default_mapsize") ## Needed for LMDB
        cl <- list(injob = create_queue(p$injob, mapsize = mapsize),
                   outjob = create_queue(p$outjob, mapsize = mapsize),
                   logfile = p$logfile,
                   env = p$env,
                   name = name)
        cl
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
#' @importFrom queue init_queue
#' @export
#'
cluster_join <- function(name) {
        if(!file.exists(name))
                stop(sprintf("cluster '%s' does not exist", name))
        p <- cluster_paths(name)
        mapsize = getOption("threadpool_default_mapsize")  ## Needed for LMDB
        list(injob = init_queue(p$injob, mapsize = mapsize),
             outjob = init_queue(p$outjob, mapsize = mapsize),
             logfile = p$logfile,
             env = p$env,
             name = name)
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
        while(!inherits(task <- cluster_next_task(cl), "try-error")) {
                result <- try({
                        msg <- capture.output({
                                taskout <- task_run(task, envir)
                        })
                        if(length(msg) > 0) {
                                message_log(cl, msg)
                        }
                        taskout
                })
                cluster_finish_task(cl, result)
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
#' @param output the output from a task
#'
#' @importFrom queue enqueue
#' @export
#'

cluster_finish_task <- function(cl, output) {
        outjob_q <- cl$outjob
        enqueue(outjob_q, output)
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
#' @export
#'
cluster_results <- function(cl) {
        output_q <- cl$outjob
        env <- new.env(size = 10000L)
        while(!inherits(try(out <- dequeue(output_q), silent = TRUE),
                        "try-error")) {
                key <- digest(out)
                env[[key]] <- out
        }
        keys <- ls(env, all.names = TRUE)
        results <- mget(keys, env)
        names(results) <- NULL
        results
}



