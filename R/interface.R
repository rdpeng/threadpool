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
        f <- match.fun(f)
        x <- as.list(x)
        cl <- cluster_create(cl_name)
        cluster_add_tasks(cl, x, f)
        exportEnv(cl, envir)
        cl_name
}

exportEnv <- function(cl, envir) {
        objnames <- ls(envir, all.names = TRUE)
        objlist <- mget(objnames, envir)
        saveRDS(objlist, cl$env, compress = FALSE)
}



#' Add a Tasks to a Cluster
#'
#' Add a batch of tasks to a cluster
#'
#' @param cl a cluster object
#' @param x the data
#' @param f function to be applied to the data
#'
#' @export

cluster_add_tasks <- function(cl, x, f) {
        f <- match.fun(f)
        x <- as.list(x)
        for(i in seq_along(x)) {
                task <- new_task(x[[i]], f)
                cluster_add1_task(cl, task)
        }
        invisible(NULL)
}






