AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
				-I${top_srcdir}/tools/bigquerytest/include/mysql \
			  -I${top_srcdir}/src

LDADD = $(top_builddir)/src/chunkserver/libchunkserver.a \
		$(top_builddir)/src/mergeserver/libmergeserver.a \
	       $(top_builddir)/tools/bigquerytest/lib/libmysqlclient_r.a\
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${EASY_LIB_PATH}/libeasy.a \
		${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lmysqlclient_r -lpthread -lc -lm -lrt -ldl -lssl -Llib -lboost_regex
#AM_LDFLAGS = -lpq -lmysqlclient -lpthread -lc -lm -lrt -ldl
CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall -DORDER_SERIALIZE #-O2 -fno-strict-aliasing
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libbigquery.a
bin_PROGRAMS = bigquery

libbigquery_a_SOURCES = \
									mysql_client.cpp \
									util.cpp \
									prefix_info.cpp \
									key_generator.cpp \
									value_generator.cpp \
									bigquery.cpp \
									bigquery_reader.cpp \
									bigquery_writer.cpp \
									read_worker.cpp \
									write_worker.cpp\
									bigquerytest_param.cpp \
									bigquerytest.cpp

bigquery_SOURCES = $(libbigquery_a_SOURCES) main.cpp

EXTRA_DIST = mysql_client.h \
						 util.h \
						 prefix_info.h \
						 key_generator.h \
						 value_generator.h \
						 bigquery.h \
						 bigquery_reader.h \
						 bigquery_writer.h \
						 read_worker.h \
						 write_worker.h \
						 bigquerytest_param.h \
						 bigquerytest.h

clean-local:
	-rm -f *.gcov *.gcno *.gcda
