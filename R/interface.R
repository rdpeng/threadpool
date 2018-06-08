## Higher level interface


#' Thread Pool Map
#'
#' Map a function in parallal across the elements of a list
#'
#' @param x an R object that will be coerced to a list
#' @param f a function to be mapped to the elements of \code{x}
#' @param envir an environment within which to evaluate the function \code{f}
#' @param cl_name an optional name for the cluster queue
#' @param ncores the number of cores to use
#'
#' @return a list containing the results
#'
#' @importFrom parallel mccollect
#'
tp_map <- function(x, f, cl_name, envir = parent.frame(),
                   ncores = 2L) {
        f <- match.fun(f)
        x <- as.list(x)
        initialize_cluster_queue(cl_name, x, f, envir)
        result <- cluster_add_nodes(cl_name, ncores)
        result
}

#' Add Nodes to a Cluster
#'
#' For an already-running cluster, add more nodes to execute tasks.
#'
#' @param cl_name name of the cluster
#' @param ncores the number of nodes to add
#'
#' @importFrom parallel mcparallel
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









