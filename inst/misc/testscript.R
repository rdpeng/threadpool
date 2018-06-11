########################################################
## Test cluster

library(threadpool)

dir()


## Generate some tasks
cl_name <- "cluster"
n <- 25
x <- seq_len(n)
x <- as.list(x)
f <- function(num) {
        pid <- Sys.getpid()
        cat("PID ", pid, " is running task ", num, "\n")
        Sys.sleep(1)
        list(output = paste0(pid, " is finished running ", num, "!"))
}

## Start up cluster
cluster_initialize(cl_name, x, f, env = globalenv())
cl <- cluster_join(cl_name)
cluster_run(cl)

r <- cluster_reduce(cl)

delete_cluster(cl_name)




########################################################


