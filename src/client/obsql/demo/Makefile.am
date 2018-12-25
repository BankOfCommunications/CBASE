cc=g++
bin_PROGRAMS=sqldemo mtsqldemo trxdemo
sqldemo_SOURCES = ob_sql_demo.cpp
mtsqldemo_SOURCES = ob_sql_demo_mt.cpp
trxdemo_SOURCES = ob_sql_demo_transaction.cpp

AM_CPPFLAGS= -I${top_srcdir}/include\
             -I${TBLIB_ROOT}/include/tbsys \
             -I${top_srcdir}/src

AM_LDFLAGS=  -L/usr/lib64/mysql \
             -L${TBLIB_ROOT}/lib/ \
             -lpthread -ltbsys -lmysqlclient -lboost_regex

LDADD = ${TBLIB_ROOT}/lib/libtbsys.a


