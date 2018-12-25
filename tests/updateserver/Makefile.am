AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/tools

LIBTOOLFLAGS=--preserve-dup-deps

LDADD = \
		$(top_builddir)/src/chunkserver/libchunkserver.a\
		$(top_builddir)/src/updateserver/libupdateserver.a \
    $(top_builddir)/src/common/roottable/libroottable.a \
		$(top_builddir)/src/mergeserver/libmergeserver.a\
		$(top_builddir)/src/sstable/libsstable.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/sql/libsql.a \
    $(top_builddir)/src/mergeserver/libmergeserver.a \
    $(top_builddir)/src/obmysql/libobmysql.a \
    $(top_builddir)/src/sql/libsql.a \
    $(top_builddir)/src/mergeserver/libmergeserver.a \
    $(top_builddir)/src/common/roottable/libroottable.a \
		$(top_builddir)/src/common/compress/libcomp.a\
    $(top_builddir)/src/common/libcommon.a \
    $(top_builddir)/src/common/btree/libbtree.a \
    $(top_builddir)/src/sql/libsql.a \
		${EASY_LIB_PATH}/libeasy.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a -lcrypt

AM_LDFLAGS=-lgtest -lpthread -lc -lm  -lrt -lboost_regex ${GCOV_LIB} -ldl -laio -lcrypt -lssl #-pg
CXXFLAGS+=-D__UNIT_TEST__ -D_BTREE_ENGINE_ -DROWKEY_IS_OBJ=1 -DUSE_LIBEASY=1 #-DNO_STAT
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

#bin_PROGRAMS = test_ups_mutator \
#               test_scan \
#               test_get \
#               test_storemgr \
#               test_sstablemgr \
#               test_rowiter \
#               test_tablemgr \
#               test_transfer_sstable_query \
#               test_memtable \
#               test_replay_local_log \
#               test_ob_pos_log_reader \
#               test_ob_log_buffer \
#               test_ob_prefetch_log_buffer \
#               test_ob_fetch_log \
#               test_trans_mgr \
#               test_session_mgr \
#               test_fifo_allocator \
#               test_lock_mgr \
#               test_lock_filter \
#               test_inc_scan \
#               test_memtable_modify \
#               test_async_rw_log \
#               get_whole_rowkey \
#               mget_perf_test

bin_PROGRAMS = test_ups_mutator \
               test_storemgr \
               test_sstablemgr \
               test_rowiter \
               test_tablemgr \
               test_transfer_sstable_query \
               test_replay_local_log \
               test_ob_pos_log_reader \
               test_ob_log_buffer \
               test_ob_prefetch_log_buffer \
               test_ob_fetch_log \
               test_resource_pool \
               test_session_mgr \
               test_fifo_allocator \
               test_lock_mgr \
               test_lock_filter \
               test_memtable_modify \
               test_log_data_writer \
               test_async_rw_log \
               test_merge_perf \
               test_ups_mvcc

test_merge_perf_SOURCES = test_merge_perf.cpp
test_ups_mutator_SOURCES = test_ups_mutator.cpp
test_scan_SOURCES = test_scan.cpp test_utils.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_get_SOURCES = test_get.cpp test_utils.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_memtable_SOURCES = test_memtable.cpp test_utils.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_storemgr_SOURCES = test_storemgr.cpp
test_sstablemgr_SOURCES = test_sstablemgr.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_rowiter_SOURCES = test_rowiter.cpp test_utils.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_tablemgr_SOURCES = test_tablemgr.cpp test_utils.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_transfer_sstable_query_SOURCES = test_transfer_sstable_query.cpp test_disk_path.cpp
test_trans_mgr_SOURCES = test_trans_mgr.cpp
test_session_mgr_SOURCES = test_session_mgr.cpp
test_fifo_allocator_SOURCES = test_fifo_allocator.cpp
test_lock_mgr_SOURCES = test_lock_mgr.cpp
test_resource_pool_SOURCES = test_resource_pool.cpp
#test_lighty_hash_SOURCES = test_lighty_hash.cpp
test_helper_src_list = test_base.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp $(top_builddir)/tools/log_tool/ob_utils.cpp
test_replay_local_log_SOURCES = test_replay_local_log.cpp test_utils2.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_ob_pos_log_reader_SOURCES = test_ob_pos_log_reader.cpp test_utils2.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
#test_ob_log_sync_SOURCES = test_ob_log_sync.cpp
test_ob_log_buffer_SOURCES = test_ob_log_buffer.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_ob_prefetch_log_buffer_SOURCES = test_ob_prefetch_log_buffer.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
#test_ob_fetch_log_LDADD = $(LDADD) utils/libutils.a
test_ob_fetch_log_SOURCES = test_ob_fetch_log.cpp test_utils2.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_log_data_writer_SOURCES = test_log_data_writer.cpp $(test_helper_src_list)
test_async_rw_log_SOURCES = test_async_rw_log.cpp test_utils2.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_lock_filter_SOURCES = test_lock_filter.cpp $(test_helper_src_list)
test_inc_scan_SOURCES = test_inc_scan.cpp $(test_helper_src_list)
test_memtable_modify_SOURCES = test_memtable_modify.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_ups_mvcc_SOURCES = test_ups_mvcc.cpp
mget_perf_test_SOURCES = mget_perf_test.cpp


EXTRA_DIST = \
  log_utils.h \
  mock_define.h \
  mock_root_server.h \
  my_ob_server.h \
  rwt.h \
  test_helper.h \
  test_utils2.h \
  mock_chunk_server2.h \
  mock_mem_iterator.h \
  mock_server.h \
  ob_final_data.h \
  test_base.h \
  test_init.h \
  test_utils.h \
  mock_client.h \
  mock_root_server2.h \
  mock_update_server2.h \
  ob_mock_ups.h \
  test_client_api.h \
  test_ups_table_mgr_helper.h \
  thread_worker.h

DIST_SUBDIRS= stress_test

check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS)

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda
