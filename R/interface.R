## Higher level interface


#' Thread Pool Map
#'
#' Map a function in parallal across the elements of a list
#'
#' @param x an R object that will be coerced to a list
#' @param f a function to be mapped to the elements of \code{x}
#' @param meta arbitrary metadata needed for applying the function \code{f}
#' @param cl_name an optional name for the cluster queue
#' @param ncores the number of cores to use
#' @param wait_for_result should we wait for all results to finish?
#' @param mapsize \code{mapsize} argument for underlying LMDB database
#'
#' @export
#'
tp_map <- function(x, f, meta = NULL, cl_name = NULL, ncores = 2L,
                   mapsize = getOption("threadpool_default_mapsize")) {
        f <- match.fun(f)
        x <- as.list(x)
        if(is.null(cl_name))
                cl_name <- tempfile("cluster")
        initialize_cluster_queue(cl_name, x, f, meta, mapsize)
        presult <- cluster_add_nodes(cl_name, ncores)
        list(cl = cl_name, nodes = presult)
}

#' Add Nodes to a Cluster
#'
#' For an already-running cluster, add more nodes to execute tasks.
#'
#' @param name name of the cluster
#' @param ncores the number of nodes to add
#'
#' @importFrom parallel mcparallel
#' @export
#'
#' @note Because of the use of \code{mcparallel}, whis may not work on Windows.
#'
cluster_add_nodes <- function(name, ncores = 1L) {
        presult <- vector("list", length = ncores)
        for(i in seq_len(ncores)) {
                presult[[i]] <- mcparallel({
                        cl <- cluster_join(name)
                        cluster_run(cl)
                })
        }
        presult
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
