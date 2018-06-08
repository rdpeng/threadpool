---
output: github_document
---

<!-- README.md is generated from README.Rmd. Please edit that file -->



# threadpool

The `threadpool` package implements a simple parallel programming backend using a thread pool model that can be dynamically resized during operation. The package builds clusters by setting up a queue of input jobs and having the individual cluster nodes retrieve jobs from the queue one at a time. The queue is stored on-disk and nodes communicate with each other via the filesystem.

A few benefits of this model of parallel computing are

* *Dynamic resource allocation*: You can modify the number of resources dedicated to a job as the job is running and as resources become available or unavailable.

* *Automatic load balancing*: Tasks are not prescheduled to specific nodes of the cluster and so long-running jobs do not prevent faster-running jobs from executing.

* *Fault tolerance and restartability*: Tasks can continue executing as nodes come in and out of the cluster. In particular, if the entire cluster goes down, the job can be automatically restarted where it left off.

* *Seamless switching to parallel mode*: It's easy to start and debug a job in single processor mode and then ramp up to parallel computing without needing to start the job over again.

This package will be most useful in situations where the jobs being run

* are embarassingly parallel, and so don't require complex communication between nodes;

* are relatively long-running, on the order of hours or days; 

* are being run on a batch/queued system where the availability of processors varies over time;

* have individual tasks that are heterogeneous in their runtimes.

## Installation

You can install threadpool from GitHub with:


```r
# install.packages("remotes")
remotes::install_github("rdpeng/threadpool")
```

The `threadpool` package makes use of the `thor` package by Rich FitzJohn. This package can be installed from GitHub with:


```r
remotes::install_github("richfitz/thor")
```

In addition, you need to install the `queue` package from GitHub with:


```r
remotes::install_github("rdpeng/queue")
```



## Example

This is a basic example of how you might invoke the `threadpool` package. The basic approach is to initialize the cluster (which also adds tasks to the cluster), join the cluster, and then run it by starting the execution of jobs. Once the cluster is finished running, you can retrieve the results. Finally, once we are finished we can delete the cluster.


```r
library(threadpool)

data(airquality)
x <- as.list(airquality)

f <- function(x) {
        mean(x, na.rm = TRUE)
}

## Initialize the cluster
cluster_initialize("my_cluster", x, f)
#> [1] "my_cluster"
cl <- cluster_join("my_cluster")

## Run jobs
cluster_run(cl)
#> Starting cluster node: 55862

## Gather the output
r <- cluster_results(cl)
r[1:3]
#> [[1]]
#> [1] 15.80392
#> 
#> [[2]]
#> [1] 42.12931
#> 
#> [[3]]
#> [1] 77.88235

## Clean up
delete_cluster("my_cluster")
```
