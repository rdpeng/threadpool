## Higher level interface


#' Thread Pool Map
#'
#' Map a function in parallal across the elements of a list
#'
#' @param x an R object that will be coerced to a list
#' @param f a function to be mapped to the elements of \code{x}
#' @param meta arbitrary metadata for the applying the function \code{f}
#' @param cl_name an optional name for the cluster queue
#' @param ncores the number of cores to use
#' @param wait_for_result should we wait for all results to finish?
#' @param mapsize \code{mapsize} argument for underlying LMDB database
#'
#' @importFrom parallel mcparallel mccollect
#' @export
#'
tp_map <- function(x, f, meta = NULL, cl_name = NULL, ncores = 2L,
                   wait_for_result = TRUE, mapsize = NULL) {
        f <- match.fun(f)
        x <- as.list(x)
        if(is.null(cl_name))
                cl_name <- tempfile("cluster")
        initialize_cluster_queue(cl_name, x, f, meta, mapsize)
        presult <- vector("list", length = ncores)
        for(i in seq_len(ncores)) {
                presult[[i]] <- mcparallel({
                        cl <- cluster_join(cl_name)
                        cluster_run(cl)
                })
        }
        if(wait_for_result)
                rvalue <- mccollect(presult)
        cl_name
}

#' Add Nodes to a Cluster
#'
#' For an already-running cluster, add more nodes to execute tasks.
#'
#' @param name name of the cluster
#' @param ncores the number of nodes to add
#'
#' @export
cluster_add_nodes <- function(name, ncores = 1L) {
        presults <- vector("list", length = ncores)
        for(i in seq_len(ncores)) {
                presults[[i]] <- mcparallel({
                        cl <- cluster_join(name)
                        cluster_run(cl)
                })
        }
}

#' Initialize Cluster Input Queue
#'
#' Intialize the cluster input queue by adding all of the jobs based on the
#' input data
#'
#' @param cl_name cluster name
#' @param x the data
#' @param f a function to map to the data
#' @param meta arbitrary metadata for the applying the function \code{f}
#' @param mapsize \code{mapsize} argument for underlying LMDB database

#' @export
#'
initialize_cluster_queue <- function(cl_name, x, f, meta, mapsize) {
        if(is.null(mapsize))
                mapsize <- getOption("threadpool_default_mapsize")
        cl <- cluster_create(cl_name, mapsize)
        for(i in seq_along(x)) {
                task <- new_task(x[[i]], f)
                cluster_add1_task(cl, task)
        }
        saveRDS(meta, cl$meta, compress = FALSE)
        invisible(NULL)
}
