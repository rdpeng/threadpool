########################################################
## Test cluster

library(threadpool)

dir()


## Generate some tasks
cl_name <- "cluster"
n <- 20
x <- seq_len(n)
x <- as.list(x)
f <- function(num) {
        pid <- Sys.getpid()
        cat("PID ", pid, " is running task ", num, "\n")
        Sys.sleep(1)
        paste0(pid, " is finished running ", num, "!")
}

## Run cluster as it should
cluster_initialize(cl_name, x, f, env = globalenv())
cl <- cluster_join(cl_name)
cluster_run(cl)

r <- cluster_reduce(cl)

cluster_shutdown(cl)
delete_cluster(cl_name)


## More debug!

cluster_initialize(cl_name, x, f, env = globalenv())
cl <- cluster_join(cl_name)

onecycle <- function(cl) {
        jt <- cluster_next_task(cl)
        print(class(jt))
        msg <- capture.output(result <- threadpool:::task_run(jt$value,
                                                              globalenv()))
        print(msg)
        cluster_finish_task(cl, jt, result)
}

replicate(n, onecycle(cl))
jq <- cl$jobqueue
is_empty_input(jq)
is_empty_output(jq)
jq$queue$list()

dequeue(jq)

delete_cluster(cl_name)

## Debug cluster
cl <- cluster_create(cl_name)
jq <- cl$jobqueue
is_empty_input(jq)
is_empty_output(jq)
cluster_add_tasks(cl, x, f)
threadpool:::exportEnv(cl, globalenv())
peek(cl$jobqueue)
jq$queue$list()

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

jt <- cluster_next_task(cl)
msg <- capture.output(result <- threadpool:::task_run(jt$value, globalenv()))
print(msg)
cluster_finish_task(cl, jt, result)
jq$queue$list()
is_empty_input(jq)
is_empty_output(jq)







########################################################


