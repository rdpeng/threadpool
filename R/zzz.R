.onLoad <- function(lib, pkg) {
        options(threadpool_default_mapsize = 2^30)
}