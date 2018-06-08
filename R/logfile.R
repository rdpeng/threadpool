## Log file management

logfile_path <- function(name, pid = Sys.getpid()) {
        file.path(name, "log",
                  sprintf("%s-%d.log", basename(name), pid))
}

logfile_create <- function(name, pid = Sys.getpid()) {
        logdir <- file.path(name, "log")

        if(!file.exists(logdir))
                dir.create(logdir)
        path <- logfile_path(name, pid)
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
        path <- file.path(name, create_log_file(name, pid))
        cmd <- sprintf("tail -f %s", path)
        system(cmd, intern = FALSE, wait = TRUEs)
}

