AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src

LIBTOOLFLAGS=--preserve-dup-deps
LDADD = $(top_builddir)/src/updateserver/libupdateserver.a \
		$(top_builddir)/src/rootserver/librootserver.a \
		$(top_builddir)/src/mergeserver/libmergeserver.a \
		$(top_builddir)/src/common/roottable/libroottable.a \
		$(top_builddir)/src/sstable/libsstable.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/compress/libcomp.a \
	        $(top_builddir)/src/sql/libsql.a              \
                $(top_builddir)/src/mergeserver/libmergeserver.a \
                $(top_builddir)/src/obmysql/libobmysql.a    \
                $(top_builddir)/src/sql/libsql.a              \
                $(top_builddir)/src/mergeserver/libmergeserver.a \
                 $(top_builddir)/src/sql/libsql.a              \
                $(top_builddir)/src/mergeserver/libmergeserver.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${EASY_LIB_PATH}/libeasy.a \
		${TBLIB_ROOT}/lib/libtbsys.a -lcrypt

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lcrypt -lssl -lboost_regex ${GCOV_LIB}
CXXFLAGS += -DORDER_SERIALIZE -D__LSYNC_DEBUG__
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = lsyncserver
lsyncserver_SOURCES = main.cpp  ob_lsync_server_main.cpp  ob_lsync_server_param.cpp  ob_lsync_server.cpp  ob_seekable_log_reader2.cpp ob_lsync_callback.cpp
nodist_lsyncserver_SOURCES = ${top_srcdir}/svn_version.cpp
noinst_LIBRARIES = liblsyncserver.a
liblsyncserver_a_SOURCES = ob_lsync_server_param.cpp  ob_lsync_server.cpp  ob_seekable_log_reader2.cpp ob_lsync_callback.cpp

pkgdata_data_DATA=lsyncserver.conf.template
pkgdata_datadir=${prefix}/etc
EXTRA_DIST=ob_lsync_server.h  ob_lsync_server_main.h  ob_lsync_server_param.h  ob_seekable_log_reader2.h \
           ob_lsync_callback.h lsyncserver.conf.template

clean-local:
	-rm -f *.gcov *.gcno *.gcda
