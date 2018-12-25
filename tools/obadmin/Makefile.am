AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/tests/updateserver \
			  -I${top_srcdir}/src \
			  -fPIC

LDADD = $(top_builddir)/src/updateserver/libupdateserver.a \
		$(top_builddir)/src/rootserver/librootserver.a \
		$(top_builddir)/src/chunkserver/libchunkserver.a  \
		$(top_builddir)/src/mergeserver/libmergeserver.a \
		$(top_builddir)/src/sstable/libsstable.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lcrypt -lssl -lreadline -lncurses -lcrypt -lssl -lboost_regex ${GCOV_LIB}
CXXFLAGS += -D_BTREE_ENGINE_
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = obadmin

obadmin_SOURCES = ob_chunk_server_client.cpp ob_update_server_client.cpp ob_root_server_client.cpp obadmin.cpp

EXTRA_DIST = \
		ob_admin_utils.h \
		ob_chunk_server_client.h \
		ob_root_server_client.h \
		ob_server_client.h \
		ob_update_server_client.h \
		ob_root_server_client.h

#pkgdata_data_DATA=oceanbase.sh
#pkgdata_datadir=${prefix}

include Flymake.mk

SUBDIRS=
clean-local:
	rm -f *.gcov *.gcno *.gcda
