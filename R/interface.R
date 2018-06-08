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
#' @note Because of the use of \code{mcparallel}, whis may not work on Windows.
#'
cluster_add_nodes <- function(cl_name, ncores = 1L) {
        presult <- vector("list", length = ncores)
        for(i in seq_len(ncores)) {
                presult[[i]] <- mcparallel({
                        cl <- cluster_join(cl_name)
                        cluster_run(cl)
                })
        }
        mccollect(presult)
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

#' @export
#'
cluster_initialize <- function(cl_name, x, f, envir = parent.frame()) {
        cl <- cluster_create(cl_name)
        for(i in seq_along(x)) {
                task <- new_task(x[[i]], f)
                cluster_add1_task(cl, task)
        }
        exportEnv(cl, envir)
        cl_name
}

exportEnv <- function(cl, envir) {
        objnames <- ls(envir, all.names = TRUE)
        objlist <- mget(objnames, envir)
        saveRDS(objlist, cl$env, compress = FALSE)
}









