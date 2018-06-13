## Higher level interface

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


