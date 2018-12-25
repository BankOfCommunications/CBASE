AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
				-I${top_srcdir}/tools/sqltest/include/mysql \
				-I${top_srcdir}/tools/sqltest/include/postgre\
			  -I${top_srcdir}/src

LDADD = $(top_builddir)/src/chunkserver/libchunkserver.a \
		$(top_builddir)/src/mergeserver/libmergeserver.a \
		$(top_builddir)/tools/sqltest/lib/libpq.a \
		$(top_builddir)/tools/sqltest/lib/libmysqlclient_r.a\
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${EASY_LIB_PATH}/libeasy.a \
		${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpq -lmysqlclient_r -lpthread -lc -lm -lrt -ldl -lboost_regex -L$(top_builddir)/tools/sqltest/lib/
#AM_LDFLAGS = -lpq -lmysqlclient -lpthread -lc -lm -lrt -ldl
CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall -DORDER_SERIALIZE #-O2 -fno-strict-aliasing
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

#bin_PROGRAMS = sqltest

noinst_LIBRARIES = libsqltest.a
bin_PROGRAMS = sqltest

libsqltest_a_SOURCES = \
									obsql_client.cpp \
									util.cpp \
									mysql_client.cpp \
									sqltest_schema.cpp \
									sql_builder.cpp \
									sqltest_dao.cpp \
									prefix_info.cpp \
									key_generator.cpp \
									read_worker.cpp \
									write_worker.cpp\
									sqltest_param.cpp \
									sqltest_pattern.cpp \
									sqltest.cpp \
									sqltest_main.cpp

sqltest_SOURCES = $(libsqltest_a_SOURCES) main.cpp

EXTRA_DIST = obsql_client.h \
						 util.h \
						 client.h \
						 mysql_client.h \
						 sqltest_schema.h \
						 sql_builder.h \
						 sqltest_dao.h \
						 prefix_info.h \
						 key_generator.h \
						 read_worker.h \
						 write_worker.h \
						 sqltest_param.h \
						 sqltest_pattern.h \
						 sqltest.h \
						 sqltest_main.h \
						 include \
						 data \
						 lib

clean-local:
	-rm -f *.gcov *.gcno *.gcda
