
cluster_paths <- function(name) {
        list(jobqueue = file.path(name, sprintf("%s.q", basename(name))),
             logfile = logfile_create(name),
             env = file.path(name, sprintf("%s.env.rds", basename(name))))
}

#' @importFrom queue create_job_queue
#'
cluster_create <- function(name) {
        dir.create(name)
        path <- cluster_paths(name)
        mapsize <- getOption("threadpool_default_mapsize") ## Needed for LMDB
        list(jobqueue = create_job_queue(path$jobqueue, mapsize = mapsize),
             logfile = path$logfile,
             env = path$env,
             name = name)
}

new_task <- function(data, func) {
        list(data = data, func = func)
}


#' @importFrom queue input2shelf
#'
cluster_next_task <- function(cl) {
        job_q <- cl$jobqueue
        job_task <- try({
                input2shelf(job_q)
        }, silent = TRUE)
        job_task
}


message_log <- function(cl, msg) {
        cat(msg, file = cl$logfile, sep = "\n", append = TRUE)
}

task_run <- function(task, envir) {
        result <- with(task, {
                do.call(func, list(data), envir = envir)
        })
        result
}

#' @importFrom queue shelf2output
#'
cluster_finish_task <- function(cl, job_task, output) {
        job_q <- cl$jobqueue
        shelf2output(job_q, job_task$key, output)
        cl
}













