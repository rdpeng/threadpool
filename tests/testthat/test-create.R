context("cluster creation")

test_that("create cluster", {
        cl_name <- tempfile()
        cl <- cluster_create(cl_name)
        expect_true(file.exists(sprintf("%s.in.q", cl_name)))
        expect_true(file.exists(sprintf("%s.out.q", cl_name)))
        expect_equal(cl$meta, sprintf("%s.meta.rds", cl_name))
        expect_is(cl$injob, "queue")
        expect_is(cl$outjob, "queue")
})
