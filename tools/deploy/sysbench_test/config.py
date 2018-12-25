# options for sysbench.
# Author: junyue

opt = dict(
    {
# options for sysbench

# where is sysbench?? If you don't specify, it'll use `which' command
# to find out.
#"sysbench-bin" : "/home/xiaojun.chengxj/tools/ProjectObTest/sysbench-0.4.12/sysbench/sysbench",
# "sysbench-lib" : "/home/xiaojun.chengxj/mysql/lib",

# number of threads to use
"num-threads" : "1",
# limit for total number of requests
"max-requests" : "10000000",
# limit for total execution time in seconds
"max-time" : "60",
# amount of time to wait after --max-time before forcing shutdown
"forced-shutdown" : "off",
# size of stack per thread
"thread-stack-size" : "32K",
# initialize random number generator
"init-rng" : "off",
# print more debugging info
"debug" : "on",
# perform validation checks where possible
"validate" : "off",

# test type to use {simple,complex,nontrx,join,sp}
"oltp-test-mode" : "complex",
# reconnect mode {session,transaction,query,random} 
"oltp-reconnect-mode" : "session",
# name of store procedure to call in SP test mode
"oltp-sp-name" : "",
# generate only 'read' queries (do not modify database)
"oltp-read-only" : "on",
# skip BEGIN/COMMIT statements
"oltp-skip-trx" : "on",
# range size for range queries
"oltp-range-size" : "100",
# number of point selects
"oltp-point-selects" : "1",
# number of simple ranges
"oltp-simple-ranges" : "0",
# number of sum ranges
"oltp-sum-ranges" : "0",
# number of ordered ranges
"oltp-order-ranges" : "0",
# number of distinct ranges
"oltp-distinct-ranges" : "0",
# number of join point selects
"oltp-join-point-selects" : "0",
# number of join simple ranges
"oltp-join-simple-ranges" : "0",
# number of join sum ranges
"oltp-join-sum-ranges" : "0",
# number of join ordered ranges
"oltp-join-order-ranges" : "0",
# number of join distinct ranges
"oltp-join-distinct-ranges" : "0",
# mode for non-transactional test {select, update_key, update_nokey, insert, delete}
"oltp-nontrx-mode" : "select",
# whether AUTO_INCREMENT (or equivalent) should be used on id column
"oltp-auto-inc" : "off",
# time in microseconds to sleep after connection to database
"oltp-connect-delay" : "10000",
# minimum time in microseconds to sleep after each request
"oltp-user-delay-min" : "0",
# maximum time in microseconds to sleep after each request
"oltp-user-delay-max" : "0",
# name of test table
"oltp-table-name" : "sbtest",
# number of records in test table
"oltp-table-size" : "10000",
# random numbers distribution {uniform,gaussian,special}
"oltp-dist-type" : "special",
# number of iterations used for numbers generation
"oltp-dist-iter" : "12",
# percentage of values to be treated as 'special'
"oltp-dist-pct" : "1",
# percentage of 'special' values to use
"oltp-dist-res" : "1",
# prepared statements usage mode {auto, disable}
"db-ps-mode" : "disable",
"mysql-user" : "admin",
"mysql-password" : "admin",
"command" : "prepare"
})

__all__ = ["opt"]
