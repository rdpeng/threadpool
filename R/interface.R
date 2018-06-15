## Higher level interface

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
#' @return the cluster object is returned
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
                        if(length(msg) > 0L) {
                                message_log(cl, msg)
                        }
                        taskout
                })
                cl <- cluster_finish_task(cl, job_task, result)
        }
        invisible(cl)
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


#' Add Nodes to a Cluster
#'
#' For an already-running cluster, add more nodes to execute tasks.
#'
#' @param cl_name name of the cluster
#' @param ncores the number of nodes to add
#'
#' @importFrom parallel mcparallel mccollect
#' @export
#'
#' @note This function will only work on macOS and Unix-alikes as it uses the
#' forking mechanism to launch the new nodes.
#'
#'
cluster_add_nodes <- function(cl_name, ncores = 1L) {
        presult <- vector("list", length = ncores)
        for(i in seq_len(ncores)) {
                presult[[i]] <- mcparallel({
                        cl <- cluster_join(cl_name)
                        cluster_run(cl)
                })
        }
        r <- mccollect(presult)
        invisible(r)
}

#' Initialize Cluster Input Queue
#'
#' Intialize the cluster input queue by adding all of the jobs based on the
#' input data
#'
#' @param cl_name cluster name
#' @param x the data
#' @param f a function to map to the data
#' @param envir an environment within which to evaluate the function \code{f}
#'
#' @return a cluster object
#' @export
#'
cluster_initialize <- function(cl_name, x, f, envir = parent.frame()) {
        f <- match.fun(f)
        x <- as.list(x)
        cl <- cluster_create(cl_name)
        cl <- cluster_add_tasks(cl, x, f)
        cl <- exportEnv(cl, envir)
        invisible(cl)
}

exportEnv <- function(cl, envir) {
        objnames <- ls(envir, all.names = TRUE)
        objlist <- mget(objnames, envir)
        saveRDS(objlist, cl$env, compress = FALSE)
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
#' @importFrom queue init_job_queue
#' @export
#'
cluster_join <- function(name) {
        if(!file.exists(name))
                stop(sprintf("cluster '%s' does not exist", name))
        path <- cluster_paths(name)
        mapsize = getOption("threadpool_default_mapsize")  ## Needed for LMDB
        list(jobqueue = init_job_queue(path$jobqueue, mapsize = mapsize),
             logfile = path$logfile,
             env = path$env,
             name = name)
}


#' Add a Tasks to a Cluster
#'
#' Add a batch of tasks to a cluster
#'
#' @param cl a cluster object
#' @param x the data
#' @param f function to be applied to the data
#'
#' @importFrom queue enqueue
#' @export

cluster_add_tasks <- function(cl, x, f) {
        f <- match.fun(f)
        x <- as.list(x)
        for(i in seq_along(x)) {
                task <- new_task(x[[i]], f)
                enqueue(cl$jobqueue, task)
        }
        invisible(cl)
}

#' Map a function to data
#'
#' Cluster version of map to map a function to data elements
#'
#' @param x the data
#' @param f function to be mapped to the data
#' @param cl_name cluster name
#' @param ncores the number of cores to uses
#' @param envir the evaluation environment
#' @param cleanup if TRUE, cluster is deleted at the end (default FALSE)
#'
#' @export
#'
cluster_map <- function(x, f, cl_name = NULL, ncores = 1L,
                        envir = parent.frame(), cleanup = FALSE) {
        f <- match.fun(f)
        x <- as.list(x)

        if(is.null(cl_name))
                cl_name <- tempfile("cluster-")
        cl <- cluster_initialize(cl_name, x, f, envir)
        out <- cluster_add_nodes(cl_name, ncores)
        results <- cluster_reduce(cl)
        if(length(results) == length(x))
                names(results) <- names(x)
        if(cleanup)
                delete_cluster(cl_name)
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
#' @return the number of items on the shelf
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
        invisible(cl)
}


#' Shutdown a Cluster
#'
#' Shutdown a cluster by closing all open threads
#'
#' @param cl a cluster object
#'
#' @export
#'
cluster_shutdown <- function(cl) {
        cl$jobqueue$queue$close()
}


#' Delete a Cluster
#'
#' Clean up cluster-related files from the filesystem
#'
#' @param name cluster name
#' @export
#'
delete_cluster <- function(name) {
        cl <- cluster_join(name)
        cluster_shutdown(cl)
        val <- unlink(name, recursive = TRUE)
        if(val > 0)
                warning(sprintf("problem deleting cluster '%s'", name))
        invisible(val)
}

