## Higher level interface

#' @param x an R object that will be coerced to a list
#' @param f a function
#' @param cl_name an option name for the cluster queue
#' @param ncores the number of cores to use
#' @param wait_for_result should we wait for all results to finish? (default = TRUE)
#' @importFrom parallel mcparallel mccollect
#' @export
#'
rw_map <- function(x, f, cl_name = NULL, ncores = 2L, wait_for_result = TRUE) {
        f <- match.fun(f)
        x <- as.list(x)
        if(is.null(cl_name))
                cl_name <- tempfile("cluster")
        initialize_cluster_queue(cl_name, x, f)
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
        rvalue
}



initialize_cluster_queue <- function(cl_name, x, f) {
        cl <- cluster_create(cl_name)
        for(i in seq_along(x)) {
                task <- new_task(x[[i]], f)
                cluster_add1_task(cl, task)
        }
        invisible(NULL)
}
