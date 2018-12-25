AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
        -I${top_srcdir}/tests/updateserver \
        -I${top_srcdir}/tests/common \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src

LIBTOOLFLAGS=--preserve-dup-deps

LDADD = $(top_builddir)/src/updateserver/libupdateserver.a \
	$(top_builddir)/src/rootserver/librootserver.a \
	$(top_builddir)/src/chunkserver/libchunkserver.a  \
	$(top_builddir)/src/mergeserver/libmergeserver.a \
        $(top_builddir)/src/common/roottable/libroottable.a \
	$(top_builddir)/src/sstable/libsstable.a \
        ${top_builddir}/src/common/libcommon.a        \
	$(top_builddir)/src/sql/libsql.a \
        $(top_builddir)/src/mergeserver/libmergeserver.a \
        $(top_builddir)/src/obmysql/libobmysql.a \
        $(top_builddir)/src/sql/libsql.a \
        $(top_builddir)/src/mergeserver/libmergeserver.a \
	$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${EASY_LIB_PATH}/libeasy.a \
		${TBLIB_ROOT}/lib/libtbsys.a -lcrypt

AM_LDFLAGS = -O2 -lpthread -lc -lm -lrt -ldl -laio -lcrypt -lssl -lboost_regex ${GCOV_LIB}
CXXFLAGS += -g -O2 -DROWKEY_IS_OBJ=1 -DUSE_LIBEASY=1
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = log_tool client
log_tool_SOURCES = log_tool.cpp ob_utils.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
client_SOURCES = ob_client2.cpp ob_utils.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp

EXTRA_DIST = cmd_args_parser.h  file_utils.h  ob_client2.h  ob_utils.h cached_item.h builder.h

clean-local:
	-rm -f *.gcov *.gcno *.gcda
