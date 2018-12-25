AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/common/hash \
			  -I$(top_srcdir)/src/common/compress \
			  -I${top_srcdir}/src/chunkserver

LIBTOOLFLAGS=--preserve-dup-deps

LDADD = $(top_builddir)/src/chunkserver/libchunkserver.a \
		$(top_builddir)/src/common/roottable/libroottable.a \
		$(top_builddir)/src/sstable/libsstable.a             \
		$(top_builddir)/src/updateserver/libupdateserver.a  \
		$(top_builddir)/src/mergeserver/libmergeserver.a \
		$(top_builddir)/src/rootserver/librootserver.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/sql/libsql.a \
		$(top_builddir)/src/sstable/libsstable.a \
		$(top_builddir)/src/compactsstable/libcompactsstable.a \
		$(top_builddir)/src/compactsstablev2/libcompactsstablev2.a \
		libtestdiskpath.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		${EASY_LIB_PATH}/libeasy.a \
		${TBLIB_ROOT}/lib/libtbnet.a\
		${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lc -lm -lrt -lgtest -ldl -lboost_regex ${GCOV_LIB} -laio -lssl
CXXFLAGS += -D__GNU_SOURCE
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libtestdiskpath.a
libtestdiskpath_a_SOURCES = test_disk_path.cpp ../sstable/test_sstable_stat.cpp

bin_PROGRAMS = test_fileinfo_cache \
			   test_root_server_rpc \
			   test_tablet_manager_get \
			   test_disk_manager \
			   test_tablet_manager \
			   test_mv_tablet_image \
			   test_block_cache_reader_loader \
			   test_query_agent \
			   test_ups_blacklist \
			   test_tablet_merge_filter

test_fileinfo_cache_SOURCES = test_fileinfocache.cpp
test_root_server_rpc_SOURCES = test_root_server_rpc.cpp
test_tablet_manager_get_SOURCES = test_tablet_manager_get.cpp
test_disk_manager_SOURCES = test_disk_manager.cpp
test_tablet_manager_SOURCES = test_tablet_manager.cpp test_helper.cpp
test_mv_tablet_image_SOURCES = test_mv_tablet_image.cpp test_helper.cpp
test_block_cache_reader_loader_SOURCES = test_block_cache_reader_loader.cpp
test_query_agent_SOURCES = test_query_agent.cpp
test_ups_blacklist_SOURCES = test_ups_blacklist.cpp
test_tablet_merge_filter_SOURCES = test_tablet_merge_filter.cpp
EXTRA_DIST = \
			 mock_root_server.h \
			 test_helper.h

check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS)
CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda
