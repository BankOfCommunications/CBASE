AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
              -I${EASY_ROOT}/include/easy   \
              -I${top_srcdir}/include       \
              -I${top_srcdir}/src           \
              -fPIC

LDADD = $(top_builddir)/src/common/compress/libcomp.a     \
        $(top_builddir)/src/mergeserver/libmergeserver.a  \
        $(top_builddir)/src/common/btree/libbtree.a       \
        $(top_builddir)/src/common/libcommon.a            \
        ${EASY_LIB_PATH}/libeasy.a                        \
        ${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lboost_regex
#CXXFLAGS+= -D_SSTABLE_NO_STAT_
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libsstable.a
libsstable_a_SOURCES =                                                 \
  ob_aio_buffer_mgr.h               ob_aio_buffer_mgr.cpp              \
  ob_aio_event_mgr.h                ob_aio_event_mgr.cpp               \
  ob_block_index_cache.h            ob_block_index_cache.cpp           \
  ob_blockcache.h                   ob_blockcache.cpp                  \
  ob_column_group_scanner.h         ob_column_group_scanner.cpp        \
  ob_disk_path.h                    ob_sstable_reader_i.h              \
  ob_scan_column_indexes.h                                             \
  ob_seq_sstable_scanner.h          ob_seq_sstable_scanner.cpp         \
  ob_sstable_block_builder.h        ob_sstable_block_builder.cpp       \
  ob_sstable_block_getter.h         ob_sstable_block_getter.cpp        \
  ob_sstable_block_index_buffer.h   ob_sstable_block_index_buffer.cpp  \
  ob_sstable_block_index_builder.h  ob_sstable_block_index_builder.cpp \
  ob_sstable_block_index_v2.h       ob_sstable_block_index_v2.cpp      \
  ob_sstable_block_reader.h         ob_sstable_block_reader.cpp        \
  ob_sstable_block_scanner.h        ob_sstable_block_scanner.cpp       \
  ob_sstable_getter.h               ob_sstable_getter.cpp              \
  ob_sstable_merger.h               ob_sstable_merger.cpp              \
  ob_sstable_reader.h               ob_sstable_reader.cpp              \
  ob_sstable_row.h                  ob_sstable_row.cpp                 \
  ob_sstable_row_cache.h            ob_sstable_row_cache.cpp           \
  ob_sstable_scan_param.h           ob_sstable_scan_param.cpp          \
  ob_sstable_scanner.h              ob_sstable_scanner.cpp             \
  ob_sstable_schema.h               ob_sstable_schema.cpp              \
  ob_sstable_schema_cache.h         ob_sstable_schema_cache.cpp        \
  ob_sstable_trailer.h              ob_sstable_trailer.cpp             \
  ob_sstable_writer.h               ob_sstable_writer.cpp

clean-local:
	-rm -f *.gcov *.gcno *.gcda
