########################################################
## Test cluster

library(threadpool)

setwd("~/tmp")
dir()

cl_name <- "cluster1"

## Generate some tasks
n <- 500
x <- seq_len(n)
x <- as.list(x)
f <- function(num) {
        pid <- Sys.getpid()
        cat("PID ", pid, " is running task ", num, "\n")
        Sys.sleep(1)
        list(output = paste0(pid, " is finished running ", num, "!"))
}

## Start up cluster
initialize_cluster_queue(cl_name, x, f, env = globalenv())
cl <- cluster_join(cl_name)
cluster_run(cl)

r <- cluster_results(cl)


library(threadpool)
cluster_add_nodes(cl_name, 3)

delete_cluster(cl_name)




########################################################


