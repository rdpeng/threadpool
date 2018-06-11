########################################################
## Test cluster

library(threadpool)
library(queue)

dir()


## Generate some tasks
cl_name <- "cluster"
n <- 10
x <- seq_len(n)
x <- as.list(x)
f <- function(num) {
        pid <- Sys.getpid()
        cat("PID ", pid, " is running task ", num, "\n")
        Sys.sleep(1)
        paste0(pid, " is finished running ", num, "!")
}

## Start up cluster
cl <- cluster_create(cl_name)
jq <- cl$jobqueue
is_empty_input(jq)
is_empty_output(jq)
cluster_add_tasks(cl, x, f)
threadpool:::exportEnv(cl, globalenv())
peek(cl$jobqueue)

jt <- cluster_next_task(cl)
jq$queue$list()
result <- threadpool:::task_run(jt$value, globalenv())
shelf2output(jq, jt$key, result)
is_empty_input(jq)
is_empty_output(jq)
shelf_list(jq)
jq$queue$list()
dequeue(jq)
jq$queue$list()

cluster_initialize(cl_name, x, f, env = globalenv())
cl <- cluster_join(cl_name)
cluster_run(cl)


r <- cluster_reduce(cl)

delete_cluster(cl_name)




########################################################


