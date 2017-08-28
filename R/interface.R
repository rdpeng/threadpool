## Higher level interface


#' @importFrom parallel mcparallel
#' @export
#'
rw_map <- function(x, f, ..., cl_name = NULL, ncores = 2L) {
        x <- as.list(x)
        if(is.null(cl_name))
                cl_name <- tempfile("cluster")
        initialize_cluster_queue(cl_name, x, f, ...)
        presult <- vector("list", length = ncores)
        for(i in seq_len(ncores)) {
                presult[[i]] <- mcparallel({
                        cl <- cluster_join(cl_name)
                        cluster_run(cl)
                })
        }
        presult
}



initialize_cluster_queue <- function(cl_name, x, f, ...) {
        cl <- cluster_create(cl_name)
        for(i in seq_along(x)) {
                task <- new_task(x[[i]], f)
                cluster_add1_task(cl, task)
        }
        invisible(NULL)
}
