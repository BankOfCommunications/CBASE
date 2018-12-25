# options for mysqltest.
# Author: Fufeng

test_set = [
    "bool",
    "alias"
    ]

exclude_set = [
    "stress_hang","ps"
    ]
opt = dict(
    {
# options for mysqltest
#
# where is mysqltest?? If you don't specify, it'll use `which' command
# to find out.
#"mysqltest-bin" : "/usr/local/bin/mysqltest",
# tmp dir to use when run `mysqltest'
"tmp-dir" : "tmp",
# var dir to use when run `mysqltest'
"var-dir" : "var",
# The host connecting to, i.e. OceanBase merge server ip(vip)
# update: no need for `deploy tool'
"host" : "localhost",
# port for mysql on Oceanbase merge server.
# update: no need for `deploy tool'
"port" : "3306",
# mysql user
"user" : "admin",
# mysql password (don't set if no password)
"password" : "admin",
# the default database to use when run `mysqltest'
"database" : "test",
# Where the test cases put? Support absolute path and relative path
# which relate to this configure file.
"test-dir" : "t",
# Where the result of cases put? Support absolute path and relative path
# which relate to this configure file.
"result-dir" : "r",
# Where to save output when should record result? Will use
# `result-dir' option if not set it. Note need set `record' option, or
# this option will be ignored. Support absolute path and relative path
# which relate to this configure file.
"record-dir" : "record",
# which tests should execute, note: it'll ignore `test-pattern' if you
# set `test-set'
#"test-set" : test_set,
# It wouldn't run if any test in the `exclude' set.
"exclude" : exclude_set,
# if set `test-set', it'll use `test-set' first and ignore
# `test-pattern'
## "test-pattern" : "*.test",
# Should record result from server?
"record" : False,
# Number of lines of resut to include in failure report
"tail-lines" : 10,
# where log put?
"log-dir" : "log",
# log name template, '%s' stand for time
"log-temp" : "mysqltest-%s.log",
})

__all__ = ["opt"]
