## Test cluster

library(rwmodel)

clname <- tempfile()
cl <- cluster_make(clname)
str(cl)
