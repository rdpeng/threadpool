context("cluster creation")

test_that("create cluster", {
        cl_name <- tempfile()
        cl <- cluster_create(cl_name)
        expect_true(file.exists(file.path(cl_name,
                                          sprintf("%s.in.q",
                                                  basename(cl_name)))))
        expect_true(file.exists(file.path(cl_name,
                                          sprintf("%s.out.q",
                                                  basename(cl_name)))))
        expect_is(cl$injob, "queue")
        expect_is(cl$outjob, "queue")
})
