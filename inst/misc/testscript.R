## Test cluster

library(threadpool)

setwd("~/tmp")
dir()

cl_name <- "cluster1"

## Generate some tasks
n <- 500
x <- seq_len(n)
x <- as.list(x)
f <- function(num, meta) {
        pid <- Sys.getpid()
        nr <- nrow(meta)
        cat("PID ", pid, " is running task ", num, ": metadata has ", nr,
            " rows\n", file = "output.log", append = TRUE)
        Sys.sleep(1)
        list(output = paste0(pid, " is finished running ", num, "!"))
}
meta <- airquality

## Start up cluster
initialize_cluster_queue(cl_name, x, f, env = globalenv(),
                         meta = airquality, mapsize = NULL)

cl <- cluster_join(cl_name, 2^30)
cluster_run(cl)


library(threadpool)
cluster_add_nodes(cl_name, 3)

delete_cluster(cl_name)

########################################################
## Use tp_map() function

setwd("~/tmp")
dir()

## Generate some tasks
n <- 500
x <- seq_len(n)
x <- as.list(x)
f <- function(num, meta) {
        pid <- Sys.getpid()
        outfile <- sprintf("output_%d.txt", pid)
        nr <- nrow(meta)
        cat(pid, "is running task", num, ": metadata has", nr, "rows\n",
            file = outfile, append = TRUE)
        Sys.sleep(1)
        list(output = paste0(pid, " is finished running ", num, "!"))
}
meta <- airquality
cl_name <- "cluster1"

tp_map(x, f, meta = meta, cl_name = cl_name, ncores = 3)


delete_cluster(cl_name)


########################################################
## Initialize cluster by hand

cl <- cluster_create(cl_name)
for(i in seq_along(x)) {
        task <- new_task(x[[i]], f)
        cluster_add1_task(cl, task)
        ## enqueue(cl$injob, task)
}
saveRDS(meta, cl$meta, compress = FALSE)


########################################################


