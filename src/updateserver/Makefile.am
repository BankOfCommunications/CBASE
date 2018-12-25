AM_CPPFLAGS := $(AM_CPPFLAGS) -I${TBLIB_ROOT}/include/tbsys  \
              -I${EASY_ROOT}/include/easy    \
              -I${top_srcdir}/include        \
              -I${top_srcdir}/src

LIBTOOLFLAGS=--preserve-dup-deps
CXXFLAGS += -D__UNIT_TEST__
#LIBTOOLFLAGS=--preserve-dup-deps

LDADD = $(top_builddir)/src/common/roottable/libroottable.a \
	$(top_builddir)/src/sql/libsql.a              \
        $(top_builddir)/src/mergeserver/libmergeserver.a \
        $(top_builddir)/src/obmysql/libobmysql.a    \
        $(top_builddir)/src/sstable/libsstable.a      \
        ${top_builddir}/src/common/compress/libcomp.a \
        ${top_builddir}/src/common/libcommon.a        \
        $(top_builddir)/src/sql/libsql.a              \
        $(top_builddir)/src/mergeserver/libmergeserver.a \
        ${top_builddir}/src/common/libcommon.a        \
        ${top_builddir}/src/common/btree/libbtree.a   \
				$(top_builddir)/src/common/timezone/libtimezone.a    \
        ${EASY_LIB_PATH}/libeasy.a                    \
        ${TBLIB_ROOT}/lib/libtbsys.a -lcrypt

CXXFLAGS+=-D__VERSION_ID__="\"updateserver 1.0.0.0\""
CXXFLAGS+=-D_BTREE_ENGINE_
CXXFLAGS+=-fPIC
#CXXFLAGS+=-pg
#CXXFLAGS+=-O2 -Wno-strict-aliasing
AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lcrypt -lssl -lboost_regex

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libupdateserver.a
libupdateserver_a_SOURCES =                                                 \
  updateserver.conf.template                                                \
                                    $(top_srcdir)/svn_version.cpp           \
  ob_async_log_applier.h            ob_async_log_applier.cpp                \
  ob_big_log_writer.h               ob_big_log_writer.cpp                   \
  ob_bit_lock.h                                                             \
  ob_btree_engine_alloc.h           ob_btree_engine_alloc.cpp               \
  ob_cached_pos_log_reader.h        ob_cached_pos_log_reader.cpp            \
  ob_data_block.h                   ob_data_block.cpp                       \
  ob_fetched_log.h                  ob_fetched_log.cpp                      \
  ob_lighty_hash.h                                                          \
  ob_located_log_reader.h           ob_located_log_reader.cpp               \
  ob_lock_mgr.h                     ob_lock_mgr.cpp                         \
  ob_log_buffer.h                   ob_log_buffer.cpp                       \
  ob_log_locator.h                                                          \
  ob_log_replay_worker.h            ob_log_replay_worker.cpp                \
  ob_log_src.h                      ob_log_src.cpp                          \
  ob_log_sync_delay_stat.h          ob_log_sync_delay_stat.cpp              \
  ob_memtable.h                     ob_memtable.cpp                         \
  ob_memtable_rowiter.h             ob_memtable_rowiter.cpp                 \
  ob_memtank.h                                                              \
  ob_multi_file_utils.h             ob_multi_file_utils.cpp                 \
  ob_obi_slave_stat.h                                                       \
  ob_on_disk_log_locator.h          ob_on_disk_log_locator.cpp              \
  ob_pos_log_reader.h               ob_pos_log_reader.cpp                   \
  ob_prefetch_log_buffer.h          ob_prefetch_log_buffer.cpp              \
  ob_query_engine.h                 ob_query_engine.cpp                     \
  ob_recent_cache.h                                                         \
  ob_remote_log_src.h               ob_remote_log_src.cpp                   \
  ob_replay_log_src.h               ob_replay_log_src.cpp                   \
  ob_ring_data_buffer.h             ob_ring_data_buffer.cpp                 \
  ob_schema_mgrv2.h                 ob_schema_mgrv2.cpp                     \
  ob_session_mgr.h                  ob_session_mgr.cpp                      \
  ob_sessionctx_factory.h           ob_sessionctx_factory.cpp               \
  ob_session_guard.h 	\
  ob_slave_sync_type.h              ob_slave_sync_type.cpp                  \
  ob_sstable_mgr.h                  ob_sstable_mgr.cpp                      \
  ob_store_mgr.h                    ob_store_mgr.cpp                        \
  ob_table_engine.h                 ob_table_engine.cpp                     \
  ob_table_mgr.h                    ob_table_mgr.cpp                        \
  ob_trans_buffer.h                 ob_trans_buffer.cpp                     \
  ob_trans_executor.h               ob_trans_executor.cpp                   \
  ob_trans_mgr.h                    ob_trans_mgr.cpp                        \
  ob_transfer_sstable_query.h       ob_transfer_sstable_query.cpp           \
  ob_update_callback.h              ob_update_callback.cpp                  \
  ob_update_server.h                ob_update_server.cpp                    \
  ob_update_server_main.h           ob_update_server_main.cpp               \
  ob_ups_cache.h                    ob_ups_cache.cpp                        \
  ob_ups_clog_status.h              ob_ups_clog_status.cpp                  \
  stress.h \
  ob_ups_compact_cell_iterator.h    ob_ups_compact_cell_iterator.cpp        \
  ob_ups_compact_cell_writer.h      ob_ups_compact_cell_writer.cpp          \
  ob_ups_fetch_runnable.h           ob_ups_fetch_runnable.cpp               \
  ob_ups_log_mgr.h                  ob_ups_log_mgr.cpp                      \
  ob_ups_log_utils.h                ob_ups_log_utils.cpp                    \
  ob_ups_mutator.h                  ob_ups_mutator.cpp                      \
  ob_ups_replay_runnable.h          ob_ups_replay_runnable.cpp              \
  ob_ups_role_mgr.h                                                         \
  ob_ups_rpc_stub.h                 ob_ups_rpc_stub.cpp                     \
  ob_ups_slave_mgr.h                ob_ups_slave_mgr.cpp                    \
  ob_ups_stat.h                     ob_ups_stat.cpp                         \
  ob_ups_table_mgr.h                ob_ups_table_mgr.cpp                    \
  ob_ups_timer_task.h               ob_ups_timer_task.cpp                   \
  ob_ups_tmps.h                                                             \
  ob_ups_utils.h                    ob_ups_utils.cpp                        \
  ob_update_server_config.h         ob_update_server_config.cpp             \
  ups_mon.h                                                                 \
  ob_ups_phy_operator_factory.h     ob_ups_phy_operator_factory.cpp         \
  ob_table_list_query.h             ob_table_list_query.cpp                 \
  ob_ups_lock_filter.h              ob_ups_lock_filter.cpp                  \
  ob_ups_inc_scan.h                 ob_ups_inc_scan.cpp                     \
  ob_clog_stat.h                    ob_clog_stat.cpp                        \
  ups_mon.h                                                                 \
  ob_memtable_modify.h              ob_memtable_modify.cpp                  \
  ob_trigger_handler.h              ob_trigger_handler.cpp                  \
  ob_update_reload_config.h         ob_update_reload_config.cpp             \
  ob_util_interface.h                                                       \
  clog_status.h                     clog_status.cpp                         \
  clog_monitor.h                                                            \
  ob_inc_seq.h                      ob_inc_seq.cpp

bin_PROGRAMS = updateserver ob_ping clog_monitor
updateserver_SOURCES = main.cpp ${libupdateserver_a_SOURCES}
#nodist_updateserver_SOURCES = $(top_srcdir)/svn_version.cpp
ob_ping_SOURCES = ups_mon.cpp
clog_monitor_SOURCES = clog_monitor.cpp ${libupdateserver_a_SOURCES}

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk

clean-local:
	-rm -f *.gcov *.gcno *.gcda
