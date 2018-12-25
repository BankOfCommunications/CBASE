AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
        -I${top_srcdir}/tests/updateserver \
			  -I${top_srcdir}/src 

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

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lboost_regex ${GCOV_LIB}
CXXFLAGS += -D__LSYNC_DEBUG__ 
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = msyncclient
msyncclient_SOURCES = main.cpp  ob_msync_client_main.cpp ob_msync_client.cpp  ob_mutator_reader.cpp
nodist_msyncclient_SOURCES = ${top_srcdir}/svn_version.cpp

pkgdata_data_DATA=msyncclient.conf.template
pkgdata_datadir=${prefix}/etc
EXTRA_DIST= ob_msync_client.h  ob_msync_client_main.h  ob_mutator_reader.h msyncclient.conf.template

clean-local:
	-rm -f *.gcov *.gcno *.gcda
