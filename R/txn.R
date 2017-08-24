## Test transactions

library(thor)

dbname <- "testdb"
db <- mdb_env(dbname)

do_write <- function(db, key, num = 1) {
        txn <- db$begin(write = TRUE)
        txn$put(key, "hello")
        Sys.sleep(num)
        txn$commit()
}

do_write(db, "a", 15)

library(parallel)
mclapply(letters[11:14], function(key) {
        do_write(db, key, num = 2)
}, mc.cores = 4)
