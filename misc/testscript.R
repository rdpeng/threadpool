## Test cluster

library(rwmodel)

setwd("~/tmp")
clname <- "cluster1"
cl <- cluster_make(clname)
str(cl)

## Generate some tasks

set.seed(2017-08-25)
seconds <- c(1, 3, 10)
probs <- c(0.6, 0.35, 0.05)
n <- 100
data <- sample(seconds, n, replace = TRUE, prob = probs)

dir.create("input")
taskfiles <- paste0("input/task", formatC(1:n, flag = "0", width = 3))
out <- mapply(function(num, task) {
        message(task)
        writeLines(as.character(num), task)
}, data, taskfiles)

## Add tasks to queue
dir.create("output")
process <- function(filename) {
        pid <- Sys.getpid()
        num <- as.numeric(readLines(filename))
        message(pid, " is running task ", basename(filename))
        Sys.sleep(num)
        writeLines(paste0(pid, " is finished running ",
                         basename(filename), "!"),
                   file.path("output", basename(filename)))
}

out <- lapply(taskfiles, function(filename) {
        message(filename)
        cluster_add1(cl, new_task(filename, process))
})


cluster_run(cl)


## Other worker!

library(rwmodel)
cl <- cluster_join("cluster1")
cluster_run(cl)

