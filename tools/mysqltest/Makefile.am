noinst_LIBRARIES = libregex.a
libregex_a_SOURCES=regex/regfree.c regex/reginit.c regex/regcomp.c regex/regerror.c regex/regexec.c
libregex_a_CFLAGS=-Wall -O2 -g -DDBUG_OFF -I${top_srcdir}/tools/mysqltest/include
libregex_a_CPPFLAGS=

bin_PROGRAMS=mysqltest
mysqltest_SOURCES=mysqltest.cc
mysqltest_CXXFLAGS=-Wall -Wno-unused-parameter -fno-implicit-templates -fno-exceptions -fno-rtti -O2 -g
mysqltest_CPPFLAGS=-DDBUG_OFF -I${top_srcdir}/tools/mysqltest/include -I${top_srcdir}/tools/mysqltest/regex -I${top_srcdir}/tools/mysqltest/sql -I${top_srcdir}/tools/mysqltest/strings -I${top_srcdir}/tools/mysqltest/cmd-line-utils -I${top_srcdir}/tools/mysqltest/client
mysqltest_LDFLAGS=-lpthread lib/libmysqlclient.a libregex.a -lz -lm -lrt -ldl -lpthread
EXTRA_DIST=client sql cmd-line-utils strings include regex
