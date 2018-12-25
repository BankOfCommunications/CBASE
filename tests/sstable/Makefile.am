AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/common/hash \
			  -I$(top_srcdir)/src/common/compress

LIBTOOLFLAGS=--preserve-dup-deps

LDADD = $(top_builddir)/src/chunkserver/libchunkserver.a \
    $(top_builddir)/src/common/roottable/libroottable.a \
		$(top_builddir)/src/mergeserver/libmergeserver.a \
		$(top_builddir)/src/sstable/libsstable.a \
    $(top_builddir)/src/updateserver/libupdateserver.a \
    $(top_builddir)/src/sstable/libsstable.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/sql/libsql.a \
		$(top_builddir)/src/compactsstable/libcompactsstable.a \
		$(top_builddir)/src/compactsstablev2/libcompactsstablev2.a \
		libtestdiskpath.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		${EASY_LIB_PATH}/libeasy.a\
		${TBLIB_ROOT}/lib/libtbnet.a\
		${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lc -lm -lrt -lgtest -ldl -lboost_regex ${GCOV_LIB} -laio -lssl
CXXFLAGS += -D__GNU_SOURCE
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libtestdiskpath.a
libtestdiskpath_a_SOURCES = test_disk_path.cpp  test_sstable_stat.cpp

bin_PROGRAMS = test_blockcache \
			   test_pthread_blockcache \
			   test_sstable_writer \
			   test_sstable_schema \
			   test_sstable_row \
			   test_sstable_block_builder \
			   test_sstable_block_index_buffer \
			   test_sstable_block_index_builder \
			   test_bloom_filter \
			   test_sstable_trailer \
			   test_sstable_reader \
			   test_sstable_getter \
			   test_sstable_block_reader \
			   test_sstable_block_scanner \
			   test_sstable_scanner \
			   test_aio_buffer_mgr  \
			   test_column_group_scanner \
			   test_sstable_schema_cache

test_blockcache_SOURCES = test_blockcache.cpp
test_pthread_blockcache_SOURCES = test_pthread_blockcache.cpp
test_sstable_reader_SOURCES = test_sstable_reader.cpp
test_sstable_writer_SOURCES = test_sstable_writer.cpp
#test_sstable_writer_perf_SOURCES = test_sstable_writer_perf.cpp
test_sstable_schema_SOURCES = test_sstable_schema.cpp \
                              ob_sstable_schemaV1.cpp
test_sstable_schema_cache_SOURCES = test_sstable_schema_cache.cpp
test_sstable_row_SOURCES = test_sstable_row.cpp test_helper.cpp
test_sstable_block_builder_SOURCES = test_sstable_block_builder.cpp
test_sstable_block_index_buffer_SOURCES = test_sstable_block_index_buffer.cpp
test_sstable_block_index_builder_SOURCES = test_sstable_block_index_builder.cpp
test_bloom_filter_SOURCES = test_bloom_filter.cpp
test_sstable_trailer_SOURCES = test_sstable_trailer.cpp \
                                 ob_sstable_trailerV1.cpp
test_sstable_getter_SOURCES = test_sstable_getter.cpp
test_sstable_block_reader_SOURCES = test_sstable_block_reader.cpp test_helper.cpp
test_sstable_block_scanner_SOURCES = test_sstable_block_scanner.cpp test_helper.cpp
test_sstable_scanner_SOURCES = test_sstable_scanner.cpp test_helper.cpp
test_aio_buffer_mgr_SOURCES = test_aio_buffer_mgr.cpp
test_column_group_scanner_SOURCES = test_column_group_scanner.cpp test_helper.cpp

EXTRA_DIST = \
			 key.h \
			 test_helper.h \
			 ob_sstable_schemaV1.h \
			 ob_sstable_trailerV1.h


check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS)

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda
