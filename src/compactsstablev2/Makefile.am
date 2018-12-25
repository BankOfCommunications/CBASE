AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
							-I${EASY_ROOT}/include/easy \
						  -I${top_srcdir}/include \
							-I${top_srcdir}/src \
						  -fPIC \
							-pg

LDADD = ${TBLIB_ROOT}/lib/libtbsys.a \
				${top_builddir}/src/common/libcommon.a \
				${top_builddir}/src/sstable/libsstable.a

AM_LDFLAGS = -lpthread -lc -lm -lrt -lcrypt -lssl -pg -lboost_regex
CXXFLAGS+= -D_SSTABLE_NO_STAT_
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS += -lgcov
endif

noinst_LIBRARIES = libcompactsstablev2.a
libcompactsstablev2_a_SOURCES = 																			 \
ob_sstable_store_struct.h 																						 \
ob_sstable_buffer.h ob_sstable_buffer.cpp                              \
ob_sstable_block.h                                                     \
ob_sstable_block_builder.h ob_sstable_block_builder.cpp                \
ob_sstable_block_endkey_builder.h ob_sstable_block_endkey_builder.cpp  \
ob_sstable_block_index_builder.h ob_sstable_block_index_builder.cpp    \
ob_sstable_table_index_builder.h ob_sstable_table_index_builder.cpp    \
ob_sstable_table_schema_builder.h ob_sstable_table_schema_builder.cpp  \
ob_sstable_table_range_builder.h ob_sstable_table_range_builder.cpp    \
ob_sstable_table.h ob_sstable_table.cpp                                \
ob_sstable_schema.h ob_sstable_schema.cpp                              \
ob_sstable.h ob_sstable.cpp                                            \
ob_compact_sstable_writer_buffer.h ob_compact_sstable_writer_buffer.cpp \
ob_compact_sstable_writer.h ob_compact_sstable_writer.cpp              \
ob_sstable_block_index_mgr.h ob_sstable_block_index_mgr.cpp            \
ob_sstable_block_index_cache.h ob_sstable_block_index_cache.cpp        \
ob_sstable_aio_event_mgr.h ob_sstable_aio_event_mgr.cpp 							 \
ob_sstable_aio_buffer_mgr.h ob_sstable_aio_buffer_mgr.cpp              \
ob_sstable_block_cache.h ob_sstable_block_cache.cpp                    \
ob_sstable_schema_cache.h ob_sstable_schema_cache.cpp                  \
ob_compact_sstable_reader.h ob_compact_sstable_reader.cpp              \
ob_sstable_block_reader.h ob_sstable_block_reader.cpp                  \
ob_sstable_block_scanner.h ob_sstable_block_scanner.cpp                \
ob_sstable_scan_column_indexes.h ob_sstable_scan_column_indexes.cpp    \
ob_compact_sstable_scanner.h ob_compact_sstable_scanner.cpp            \
ob_sstable_disk_path.h                                                 \
ob_sstable_block_getter.h ob_sstable_block_getter.cpp                  \
ob_compact_sstable_getter.h ob_compact_sstable_getter.cpp

#ob_sstable_scan_param.h ob_sstable_scan_param.cpp
#ob_sstable_row_cache.h ob_sstable_row_cache.cpp

clean-local:
	-rm -f *.gcov *.gcno *.gcda
