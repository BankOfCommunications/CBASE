AM_CPPFLAGS := $(AM_CPPFLAGS) -I${TBLIB_ROOT}/include/tbsys \
              -I${EASY_ROOT}/include/easy   \
              -I${top_srcdir}/include       \
              -I${top_srcdir}/src

LIBTOOLFLAGS=--preserve-dup-deps

LDADD = $(top_builddir)/src/common/roottable/libroottable.a \
		$(top_builddir)/src/sstable/libsstable.a                \
		$(top_builddir)/src/compactsstable/libcompactsstable.a  \
        $(top_builddir)/src/common/libcommon.a                  \
        $(top_builddir)/src/sql/libsql.a                        \
		$(top_builddir)/src/compactsstablev2/libcompactsstablev2.a  \
        $(top_builddir)/src/common/compress/libcomp.a           \
        $(top_builddir)/src/common/btree/libbtree.a             \
        $(top_builddir)/src/common/libcommon.a                  \
        $(top_builddir)/src/sql/libsql.a                        \
				$(top_builddir)/src/common/timezone/libtimezone.a       \
        ${EASY_LIB_PATH}/libeasy.a                              \
        ${TBLIB_ROOT}/lib/libtbsys.a


AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lssl -lboost_regex
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libchunkserver.a


libchunkserver_a_SOURCES =                                               \
 chunkserver.conf.template                                               \
                                  $(top_srcdir)/svn_version.cpp          \
 ob_block_cache_loader.h          ob_block_cache_loader.cpp              \
 ob_block_cache_reader.h          ob_block_cache_reader.cpp              \
 ob_cell_array_helper.h           ob_cell_array_helper.cpp               \
 ob_cell_stream.h                 ob_cell_stream.cpp                     \
 ob_chunk_callback.h              ob_chunk_callback.cpp                  \
 ob_chunk_merge.h                 ob_chunk_merge.cpp                     \
 ob_tablet_merger_v1.h            ob_tablet_merger_v1.cpp                \
 ob_tablet_merger_v2.h            ob_tablet_merger_v2.cpp                \
 ob_chunk_server.h                ob_chunk_server.cpp                    \
 ob_chunk_server_main.h           ob_chunk_server_main.cpp               \
 ob_chunk_server_merger_proxy.h   ob_chunk_server_merger_proxy.cpp       \
 ob_chunk_server_stat.h           ob_chunk_server_stat.cpp               \
 ob_chunk_service.h               ob_chunk_service.cpp                   \
 ob_compactsstable_cache.h        ob_compactsstable_cache.cpp            \
 ob_disk_manager.h                ob_disk_manager.cpp                    \
 ob_file_recycle.h                ob_file_recycle.cpp                    \
 ob_fileinfo_cache.h              ob_fileinfo_cache.cpp                  \
 ob_get_cell_stream.h             ob_get_cell_stream.cpp                 \
 ob_get_cell_stream_wrapper.h     ob_get_cell_stream_wrapper.cpp         \
 ob_get_param_cell_iterator.h     ob_get_param_cell_iterator.cpp         \
 ob_get_scan_proxy.h              ob_get_scan_proxy.cpp                  \
 ob_join_cache.h                  ob_join_cache.cpp                      \
 ob_join_operator.h               ob_join_operator.cpp                   \
 ob_merge_join_operator.h         ob_merge_join_operator.cpp             \
 ob_merge_operator.h              ob_merge_operator.cpp                  \
 ob_merge_reader.h                ob_merge_reader.cpp                    \
 ob_multi_tablet_merger.h         ob_multi_tablet_merger.cpp             \
 ob_query_agent.h                 ob_query_agent.cpp                     \
 ob_query_service.h               ob_query_service.cpp                   \
 ob_read_param_modifier.h         ob_read_param_modifier.cpp             \
 ob_read_ups_balance.h            ob_read_ups_balance.cpp                \
 ob_row_cell_vec.h                ob_row_cell_vec.cpp                    \
 ob_rpc_proxy.h                   ob_rpc_proxy.cpp                       \
 ob_scan_cell_stream.h            ob_scan_cell_stream.cpp                \
 ob_schema_manager.h                                                     \
 ob_schema_task.h                 ob_schema_task.cpp                     \
 ob_sql_query_service.h                                                  \
 ob_sql_rpc_stub.h                ob_sql_rpc_stub.cpp                    \
 ob_switch_cache_utility.h        ob_switch_cache_utility.cpp            \
 ob_tablet.h                      ob_tablet.cpp                          \
 ob_tablet_image.h                ob_tablet_image.cpp                    \
 ob_tablet_manager.h              ob_tablet_manager.cpp                  \
 ob_tablet_merge_filter.h         ob_tablet_merge_filter.cpp             \
 ob_ups_blacklist.h               ob_ups_blacklist.cpp                   \
 ob_chunk_server_config.h         ob_chunk_server_config.cpp             \
 ob_bypass_sstable_loader.h       ob_bypass_sstable_loader.cpp           \
 ob_tablet_service.h              ob_tablet_service.cpp                  \
 ob_tablet_writer.h               ob_tablet_writer.cpp                   \
 ob_tablet_reader.h               ob_tablet_reader.cpp                   \
 ob_chunk_reload_config.h         ob_chunk_reload_config.cpp	ob_chunk_index_worker.h	ob_chunk_index_worker.cpp	ob_index_builder.h	ob_index_builder.cpp	ob_cs_query_agent.h	ob_cs_query_agent.cpp	ob_cs_scan_cell_stream.h	ob_cs_scan_cell_stream.cpp ob_agent_scan.h ob_agent_scan.cpp

bin_PROGRAMS = chunkserver
chunkserver_SOURCES = ob_disk_path.cpp main.cpp ${libchunkserver_a_SOURCES}

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk

#pkgdata_data_DATA=chunkserver.conf.template
#pkgdata_datadir=${prefix}/etc
clean-local:
	-rm -f *.gcov *.gcno *.gcda
