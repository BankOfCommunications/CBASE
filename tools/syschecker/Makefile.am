AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src

LIBTOOLFLAGS=--preserve-dup-deps

LDADD = $(top_builddir)/src/chunkserver/libchunkserver.a \
		$(top_builddir)/src/mergeserver/libmergeserver.a \
		$(top_builddir)/tools/syschecker/client/libclient.a \
		$(top_builddir)/src/rootserver/librootserver.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/sql/libsql.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${EASY_LIB_PATH}/libeasy.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -lssl -laio -lboost_regex
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = syschecker

syschecker_SOURCES = main.cpp \
					 ob_syschecker_main.cpp \
					 ob_syschecker.cpp \
					 ob_syschecker_param.cpp \
					 ob_write_worker.cpp \
					 ob_read_worker.cpp \
					 ob_syschecker_stat.cpp \
					 ob_syschecker_rule.cpp \
					 ob_syschecker_schema.cpp

EXTRA_DIST = ob_syschecker_main.h \
			 ob_syschecker.h \
			 ob_syschecker_param.h \
			 ob_write_worker.h \
			 ob_read_worker.h \
			 ob_syschecker_stat.h \
			 ob_syschecker_rule.h \
			 ob_syschecker_schema.h

SUBDIRS=client
clean-local:
	-rm -f *.gcov *.gcno *.gcda
