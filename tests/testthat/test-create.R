context("cluster creation")

test_that("create cluster", {
        cl_name <- tempfile()
        cl <- cluster_create(cl_name)
        expect_true(file.exists(file.path(cl_name,
                                          sprintf("%s.q",
                                                  basename(cl_name)))))
        expect_is(cl$jobqueue, "job_queue")
})
