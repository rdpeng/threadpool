## Log file management

logfile_path <- function(name, pid = Sys.getpid()) {
        logdir <- logfile_dir(name)
        file.path(logdir, sprintf("%s-%d.log", basename(name), pid))
}

logfile_dir <- function(name) {
        file.path(name, "log")
}

logfile_create <- function(name, pid = Sys.getpid()) {
        logdir <- logfile_dir(name)

        if(!file.exists(logdir))
                dir.create(logdir, recursive = TRUE)
        path <- logfile_path(name, pid)

        if(!file.exists(path))
                file.create(path)
        path
}

#' Show cluster node log file
#'
#' Show the output being sent to the cluster node log file
#'
#' @param name name of cluster
#' @param pid process ID for cluster node
#'
#' @export

logfile_show <- function(name, pid) {
        if(missing(pid) && interactive()) {
                logfiles <- list.files(logfile_dir(name), full.names = TRUE)

                if(length(logfiles) > 0) {
                        idx <- seq_along(logfiles)
                        cat(paste(idx, basename(logfiles)), sep = "\n")
                        num <- readline("Which log file? ")
                        num <- as.integer(num)
                        if(!(num %in% idx))
                                stop("invalid selection")
                        path <- logfiles[num]
                }
        } else
                path <- logfile_path(name, pid)
        cmd <- sprintf("tail -f %s", path)
        system(cmd, intern = FALSE, wait = TRUE)
}

