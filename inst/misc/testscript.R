## Test cluster

library(threadpool)




setwd("~/tmp")

cl_name <- "cluster1"

## Generate some tasks
n <- 500
x <- seq_len(n)
x <- as.list(x)
f <- function(num, meta) {
        pid <- Sys.getpid()
        nr <- nrow(meta)
        message(pid, " is running task ", num, ": metadata has ", nr, " rows")
        Sys.sleep(1)
        list(output = paste0(pid, " is finished running ", num, "!"))
}
meta <- airquality

## Start up cluster
initialize_cluster_queue(cl_name, x, f, meta = airquality)

cl <- cluster_join(cl_name)
cluster_run(cl)


library(threadpool)
cl <- cluster_join("cluster1")
cluster_run(cl)



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


