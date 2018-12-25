AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src\
				-I$(top_srcdir)/tests/updateserver

LDADD = \
		$(top_builddir)/src/chunkserver/libchunkserver.a\
		$(top_builddir)/src/mergeserver/libmergeserver.a\
    $(top_builddir)/src/updateserver/libupdateserver.a\
		$(top_builddir)/src/sstable/libsstable.a\
    $(top_builddir)/src/common/btree/libbtree.a \
    $(top_builddir)/src/common/libcommon.a \
    $(top_builddir)/src/common/compress/libcomp.a\
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS=-lpthread -lc -lm -lrt ${GCOV_LIB} -ldl -laio -lboost_regex
CXXFLAGS=-D__STDC_LIMIT_MACROS -g -Wall -D__UNIT_TEST__ -D_BTREE_ENGINE_
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = update_stress_test test_add perf_test

update_stress_test_SOURCES = update_stress_test.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
perf_test_SOURCES = perf_test.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp

test_add_SOURCES = main.cpp \
									 ob_schema_proxy.cpp \
									 ob_update_gen.cpp \
									 ob_scan_gen.cpp \
									 ob_executor.cpp \
									 ob_add_runnable.cpp \
                   $(top_builddir)/src/updateserver/ob_ups_stat.cpp

check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS) 

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda
