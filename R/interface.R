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
#' @importFrom parallel mcparallel mccollect
#' @export
#'
tp_map <- function(x, f, meta = NULL, cl_name = NULL, ncores = 2L,
                   wait_for_result = TRUE) {
        f <- match.fun(f)
        x <- as.list(x)
        if(is.null(cl_name))
                cl_name <- tempfile("cluster")
        initialize_cluster_queue(cl_name, x, f, meta)
        presult <- vector("list", length = ncores)
        for(i in seq_len(ncores)) {
                presult[[i]] <- mcparallel({
                        cl <- cluster_join(cl_name)
                        cluster_run(cl)
                })
        }
        if(wait_for_result)
                rvalue <- mccollect(presult)
        else
                rvalue <- presult
        invisible(rvalue)
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
#'
#' @export
#'
initialize_cluster_queue <- function(cl_name, x, f, meta) {
        cl <- cluster_create(cl_name)
        for(i in seq_along(x)) {
                task <- new_task(x[[i]], f)
                cluster_add1_task(cl, task)
        }
        saveRDS(meta, cl$meta, compress = FALSE)
        invisible(NULL)
}
