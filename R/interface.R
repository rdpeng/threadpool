## Higher level interface


#' @importFrom parallel mclapply
#' @export
#'
rw_map <- function(x, f, ..., cl_name = NULL, ncores = 2L) {
        x <- as.list(x)
        if(is.null(cl_name))
                clname <- tempfile("cluster")
        cl <- cluster_create(cl_name)
        for(i in seq_along(x)) {
                task <- new_task(x[[i]], f)
                cluster_add1_task(cl, task)
        }
        presult <- vector("list", length = ncores)
        for(i in seq_len(ncores)) {
                presult[[i]] <- mcparallel(cluster_run(cl))
        }
        presult
}

