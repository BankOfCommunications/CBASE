AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${EASY_ROOT}/include/easy \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/common/hash
LIBTOOLFLAGS=--preserve-dup-deps
LDADD = ${top_builddir}/src/common/libcommon.a \
    ${top_builddir}/src/sql/libsql.a \
    ${top_builddir}/src/common/libcommon.a \
    ${top_builddir}/src/updateserver/libupdateserver.a \
    ${EASY_LIB_PATH}/libeasy.a \
    ${TBLIB_ROOT}/lib/libtbsys.a -lcrypt

AM_LDFLAGS=-lpthread -lc -lm -lrt -lgtest -lboost_regex  ${GCOV_LIB} -lcrypt -lssl -laio
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov -fprofile-arcs
endif

bin_PROGRAMS = limit_array_test                           \
                           test_array_lock                \
                           test_cluster_server            \
                           test_server_ext                \
                           test_get_param                 \
                           test_cell_array                \
                           schema_test                    \
                           serialization_test             \
                           test_lrucache                  \
                           test_mutator                   \
                           memory_pool_test               \
                           test_ob_vector                 \
                           test_ob_cond_info              \
                           test_ob_string_buf             \
                           test_ob_object                 \
                           test_ob_scanner                \
                           test_probability_random        \
                           test_ob_scanner                \
                           test_ob_log_dir_scanner        \
                           test_scan_param                \
                           test_ob_slave_mgr              \
                           test_kr                        \
                           test_slab                      \
                           test_ob_string_search          \
                           test_groupby_param             \
                           test_counter                   \
                           test_file                      \
                           test_row_compaction            \
                           test_ob_composite_column_infix \
                           test_spop_spush_queue          \
                           test_login_mgr                 \
                           test_token                     \
                           test_meta_cache                \
                           test_schema_table              \
                           test_merger                    \
                           test_ring_buffer               \
                           test_file                      \
                           test_row_compaction            \
                           ob_flag_test                   \
                           ob_array_test                  \
                           test_nb_accessor               \
                           test_compact_cell              \
                           test_buffer_helper             \
                           ob_number_test                 \
                           ob_expr_obj_test               \
                           test_ob_row_store              \
                           ob_ups_row_util_test           \
                           ob_row_fuse_test               \
                           test_ob_postfix_expression     \
                           test_rowkey_helper             \
                           test_rowkey                    \
                           test_ob_log_generator          \
                           test_qlock                     \
                           test_ob_seq_queue              \
                           test_mutex_scheduling           \
                           test_stack_allocator           \
                           test_tsi_block_allocator       \
                           test_ob_composite_column       \
                           test_row_util                  \
                           test_ob_new_scanner            \
                           ob_strings_test                \
                           test_ob_file_service           \
                           test_ob_common_param           \
                           ob_obj_cast_test               \
                           test_cell_new_scanner          \
                           test_ob_privilege              \
                           test_ob_privilege_manager      \
                           ob_sql_get_param_test          \
                           ob_se_array_test               \
                           test_system_config             \
                           test_ob_log_reader             \
                           ob_row_test                    \
                           test_new_scanner_helper        \
                           test_iterator_adaptor          \
                           test_system_config             \
                           test_ob_config                 \
                           test_ob_stat                   \
                           test_ob_kv_storecache_perf     \
                           test_ob_packet_queue_thread_perf \
                           test_pthread_mutex_perf        \
                           test_pthread_cond_perf         \
                           test_queue_thread              \
                           test_balance_filter						\
                           test_futex_perf\
                           test_ob_tbnet_callback\
                           ob_tc_factory_test             \
                           test_ob_trace_log


test_ob_config_SOURCES = test_ob_config.cpp
test_cluster_server_SOURCES = test_cluster_server.cpp
test_system_config_SOURCES = test_system_config.cpp
test_array_lock_SOURCES = test_array_lock.cpp
test_mutator_SOURCES = test_mutator.cpp
test_token_SOURCES = test_token.cpp
test_get_param_SOURCES = test_get_param.cpp
schema_test_SOURCES = schema_test.cpp
limit_array_test_SOURCES = limit_array_test.cpp
serialization_test_SOURCES = serialization_test.cpp
memory_pool_test_SOURCES = memory_pool_test.cpp
test_lrucache_SOURCES = test_lrucache.cpp
test_ob_cond_info_SOURCES = test_ob_cond_info.cpp
test_ob_vector_SOURCES = test_ob_vector.cpp
test_ob_string_buf_SOURCES=test_ob_string_buf.cpp
test_probability_random_SOURCES = test_probability_random.cpp
test_ob_object_SOURCES = test_ob_object.cpp $(top_srcdir)/src/common/ob_object.cpp
test_scan_param_SOURCES=test_scan_param.cpp
test_ob_stat_SOURCES=test_ob_stat.cpp
test_ob_log_dir_scanner_SOURCES=test_ob_log_dir_scanner.cpp
#test_ob_single_log_reader_SOURCES= test_ob_single_log_reader.cpp
#test_ob_range_SOURCES = test_ob_range.cpp
test_ob_log_reader_SOURCES = test_ob_log_reader.cpp
#test_ob_log_writer_SOURCES = test_ob_log_writer.cpp
test_ob_slave_mgr_SOURCES = test_ob_slave_mgr.cpp
test_ob_scanner_SOURCES = test_ob_scanner.cpp
test_ob_string_search_SOURCES = test_ob_string_search.cpp
test_slab_SOURCES = test_slab.cpp
test_groupby_param_SOURCES =  test_groupby_param.cpp
test_kr_SOURCES = test_kr.cpp
test_file_SOURCES = test_file.cpp
test_row_compaction_SOURCES = test_row_compaction.cpp
test_ob_composite_column_SOURCES = test_ob_composite_column.cpp
test_ob_composite_column_infix_SOURCES = test_ob_composite_column_infix.cpp
test_ob_postfix_expression_SOURCES = test_ob_postfix_expression.cpp
test_spop_spush_queue_SOURCES = test_spop_spush_queue.cpp
test_cell_array_SOURCES = test_cell_array.cpp
test_counter_SOURCES = test_counter.cpp
test_login_mgr_SOURCES = test_login_mgr.cpp
test_meta_cache_SOURCES = test_meta_cache.cpp
test_schema_table_SOURCES = test_schema_table.cpp
test_server_ext_SOURCES = test_server_ext.cpp
ob_flag_test_SOURCES = ob_flag_test.cpp
test_merger_SOURCES = test_merger.cpp
test_ring_buffer_SOURCES = test_ring_buffer.cpp
ob_array_test_SOURCES = ob_array_test.cpp
test_nb_accessor_SOURCES = test_nb_accessor.cpp
test_compact_cell_SOURCES = test_compact_cell.cpp
test_buffer_helper_SOURCES = test_buffer_helper.cpp
ob_number_test_SOURCES = ob_number_test.cpp
test_new_scanner_helper_SOURCES = test_new_scanner_helper.cpp
ob_ups_row_util_test_SOURCES = ob_ups_row_util_test.cpp
ob_row_fuse_test_SOURCES = ob_row_fuse_test.cpp
ob_expr_obj_test_SOURCES = ob_expr_obj_test.cpp
test_ob_row_store_SOURCES = test_ob_row_store.cpp
test_qlock_SOURCES = test_qlock.cpp
test_ob_log_generator_SOURCES = test_ob_log_generator.cpp
test_ob_seq_queue_SOURCES = test_ob_seq_queue.cpp
test_stack_allocator_SOURCES = test_stack_allocator.cpp
test_mutex_scheduling_SOURCES = test_mutex_scheduling.cpp
test_tsi_block_allocator_SOURCES = test_tsi_block_allocator.cpp
test_rowkey_helper_SOURCES = test_rowkey_helper.cpp test_rowkey_helper.h
test_rowkey_SOURCES = test_rowkey.cpp
ob_strings_test_SOURCES=ob_strings_test.cpp
test_row_util_SOURCES = test_row_util.cpp
test_ob_new_scanner_SOURCES = test_ob_new_scanner.cpp
test_ob_file_service_SOURCES = test_ob_file_service.cpp
test_ob_common_param_SOURCES= test_ob_common_param.cpp
test_cell_new_scanner_SOURCES = test_cell_new_scanner.cpp
ob_obj_cast_test_SOURCES=ob_obj_cast_test.cpp
test_ob_privilege_SOURCES=test_ob_privilege.cpp
test_iterator_adaptor_SOURCES=test_iterator_adaptor.cpp
test_ob_privilege_manager_SOURCES=test_ob_privilege_manager.cpp
ob_row_test_SOURCES=ob_row_test.cpp
ob_se_array_test_SOURCES=ob_se_array_test.cpp
ob_sql_get_param_test_SOURCES=ob_sql_get_param_test.cpp
test_ob_kv_storecache_perf_SOURCES=test_ob_kv_storecache_perf.cpp
test_ob_packet_queue_thread_perf_SOURCES=test_ob_packet_queue_thread_perf.cpp
test_pthread_mutex_perf_SOURCES=test_pthread_mutex_perf.cpp
test_pthread_cond_perf_SOURCES=test_pthread_cond_perf.cpp
test_queue_thread_SOURCES = test_queue_thread.cpp
test_balance_filter_SOURCES = test_balance_filter.cpp
test_futex_perf_SOURCES=test_futex_perf.cpp
ob_tc_factory_test_SOURCES=ob_tc_factory_test.cpp

test_ob_tbnet_callback_SOURCES=test_ob_tbnet_callback.cpp
test_ob_trace_log_SOURCES=test_ob_trace_log.cpp
SUBDIRS = hash compress

DIST_SUBDIRS = hash compress

EXTRA_DIST = \
	mock_mem_iterator.h \
	test_helper.h\
	test_base.h  \
	test_rowkey_helper.h \
	slave_mgr_4_test.h

check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS)

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda
