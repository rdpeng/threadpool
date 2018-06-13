test_that("cluster run", {
        cl_name <- "cluster"
        n <- 20L
        x <- as.list(seq_len(n))
        f <- function(num) {
                pid <- Sys.getpid()
                cat("PID ", pid, " is running task ", num, "\n")
                paste0(pid, " is finished running ", num, "!")
        }

        ## Run cluster as it should
        cl <- cluster_initialize(cl_name, x, f, env = globalenv())
        cl <- cluster_run(cl)
        r <- cluster_reduce(cl)
        expect_equal(length(r), n)

        delete_cluster(cl_name)
})


test_that("cluster map", {
        x <- 1:100
        f <- function(x) {
                as.character(x)
        }
        res <- cluster_map(x, f, ncores = 2L)
        res <- unlist(res)
        expect_equal(length(res), length(x))
        expect_is(res, "character")
})