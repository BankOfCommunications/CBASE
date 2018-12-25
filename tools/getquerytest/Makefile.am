AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
				-I${top_srcdir}/tools/getquerytest/include/mysql \
			  -I${top_srcdir}/src

LDADD = $(top_builddir)/src/chunkserver/libchunkserver.a \
		$(top_builddir)/src/mergeserver/libmergeserver.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${EASY_LIB_PATH}/libeasy.a \
		${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -lboost_regex -L/usr/lib64/mysql/ -lmysqlclient
AM_CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall -DORDER_SERIALIZE #-O2 -fno-strict-aliasing
if COVERAGE
AM_CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = getquery
getquery_SOURCES = getquery.cpp \
									get_query_test.h get_query_test.cpp\
									ob_sql_client.h ob_sql_client.cpp \
									ob_sql_generator.h  ob_sql_generator.cpp \
									ob_sql_db_creater.h ob_sql_db_creater.cpp \
									ob_sql_writer.h ob_sql_writer.cpp \
									ob_sql_reader.h ob_sql_reader.cpp


check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS)

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda
