AM_CPPFLAGS := $(AM_CPPFLAGS) -I${TBLIB_ROOT}/include/tbsys\
             -I${EASY_ROOT}/include/easy\
             -I${top_srcdir}/include\
             -I${top_srcdir}/src\
             -fPIC

LDADD = ${EASY_LIB_PATH}/libeasy.a \
        ${TBLIB_ROOT}/lib/libtbsys.a \
        $(top_builddir)/src/common/btree/libbtree.a \
        ${top_builddir}/src/sql/libsql.a

AM_LDFLAGS=-lpthread -lc -lm  -lrt -lcrypt -lssl -laio -lboost_regex #-pg
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libcommon.a

common_sources=				                                                  \
  base_main.h                      base_main.cpp                        \
  batch_packet_queue_thread.h      batch_packet_queue_thread.cpp        \
  bloom_filter.h                   bloom_filter.cpp                     \
  check_sum.h                                                           \
  data_buffer.h                                                         \
  debug.h                                                               \
  ob_preload.h                                                               \
  gperf.h 								\
  dlist.h                          dlist.cpp                            \
  file_directory_utils.h           file_directory_utils.cpp             \
  file_utils.h                     file_utils.cpp                       \
  futex_sem.h                      futex_sem.cpp                        \
  hash/ob_hashmap.h                                                     \
  hash/ob_hashset.h                                                     \
  hash/ob_hashtable.h                                                   \
  hash/ob_hashutils.h                                                   \
  hash/ob_serialization.h                                               \
  hash/ob_placement_hashset.h                                           \
  hash/ob_placement_hashmap.h                                           \
	ob_fifo_allocator.h							 ob_fifo_allocator.cpp                \
	ob_id_map.h                                                           \
	ob_packet_lighty_queue.h         ob_packet_lighty_queue.cpp           \
	cmbtree/btree_base.h                                                  \
	cmbtree/btree_counter.h                                               \
	cmbtree/btree_default_alloc.h                                         \
	cmbtree/btree_define.h                                                \
	cmbtree/btree_handle.h                                                \
	cmbtree/btree_mem_pool.h                                              \
	cmbtree/btree_mutex.h                                                 \
	cmbtree/btree_node.h                                                  \
	cmbtree/btree_read_param.h                                            \
	cmbtree/btree_recycle_node.h                                          \
	cmbtree/btree_recycle_pool.h                                          \
	cmbtree/btree_thread_store.h                                          \
	cmbtree/btree_tid.h                                                   \
	cmbtree/btree_write_param.h                                           \
	cmbtree/qlock.h                                                       \
	cmbtree/thread.h                                                      \
  limit_array.h                                                         \
  murmur_hash.h                    murmur_hash.cpp                      \
  ob_action_flag.h                                                      \
  ob_iarray.h                                                           \
  ob_array.h                                                            \
  ob_array_helper.h                                                     \
  ob_array_lock.h                  ob_array_lock.cpp                    \
  ob_atomic.h                                                           \
  ob_atomic_type.h                                                      \
  ob_base_client.h                 ob_base_client.cpp                   \
  ob_base_server.h                 ob_base_server.cpp                   \
  ob_bit_set.h                                                          \
  ob_bitmap.h                                                           \
  ob_bloomfilter.h                                                      \
  ob_buffer.h                                                           \
  ob_bypass_task_info.h  ob_bypass_task_info.cpp \
	ob_buffer_helper.h                                                    \
  ob_cache.h                       ob_cache.cpp                         \
  ob_cached_allocator.h                                                 \
  ob_cell_array.h                  ob_cell_array.cpp                    \
  ob_cell_meta.h                                                        \
  ob_check_runnable.h              ob_check_runnable.cpp                \
  ob_cipher.h                                                           \
  ob_client_helper.h               ob_client_helper.cpp                 \
  ob_client_manager.h              ob_client_manager.cpp                \
  ob_client_proxy.h                ob_client_proxy.cpp                  \
  ob_client_wait_obj.h             ob_client_wait_obj.cpp               \
  ob_column_filter.h                                                    \
  ob_common_param.h                ob_common_param.cpp                  \
  ob_common_rpc_stub.h             ob_common_rpc_stub.cpp               \
  ob_compact_cell_iterator.h       ob_compact_cell_iterator.cpp         \
  ob_compact_cell_util.h           ob_compact_cell_util.cpp             \
  ob_compact_cell_writer.h         ob_compact_cell_writer.cpp           \
  ob_compact_store_type.h                                               \
  ob_compose_operator.h            ob_compose_operator.cpp              \
  ob_composite_column.h            ob_composite_column.cpp              \
  ob_cond_info.h                   ob_cond_info.cpp                     \
  ob_counter.h                     ob_counter.cpp                       \
  ob_crc64.h                       ob_crc64.cpp                         \
  ob_cur_time.h                    ob_cur_time.cpp                      \
  ob_decimal_helper.h              ob_decimal_helper.cpp                \
  ob_define.h                                                           \
  ob_delay_guard.h                                                      \
  ob_direct_log_reader.h           ob_direct_log_reader.cpp             \
  ob_easy_array.h                                                       \
  ob_easy_log.h                    ob_easy_log.cpp                      \
  ob_encrypt.h                     ob_encrypt.cpp                       \
  ob_endian.h                                                           \
  ob_expr_obj.h                    ob_expr_obj.cpp                      \
  ob_expression.h                                                       \
  ob_extra_tables_schema.h         ob_extra_tables_schema.cpp           \
  ob_fetch_runnable.h              ob_fetch_runnable.cpp                \
  ob_file.h                        ob_file.cpp                          \
  ob_file_client.h                 ob_file_client.cpp                   \
  ob_file_service.h                ob_file_service.cpp                  \
  ob_fileinfo_manager.h                                                 \
  ob_fixed_queue.h                                                      \
  ob_single_pop_queue.h                                                 \
  ob_resource_pool.h                                                    \
  ob_flag.h                        ob_flag.cpp                          \
  ob_get_param.h                   ob_get_param.cpp                     \
  ob_groupby.h                     ob_groupby.cpp                       \
  ob_groupby_operator.h            ob_groupby_operator.cpp              \
  ob_hint.h                        ob_hint.cpp                          \
  ob_infix_expression.h            ob_infix_expression.cpp              \
  ob_iterator.h                                                         \
  ob_kv_storecache.h                                                    \
  ob_lease_common.h                ob_lease_common.cpp                  \
  ob_lighty_queue.h                ob_lighty_queue.cpp                  \
  ob_link.h                                                             \
  ob_list.h                                                             \
  ob_log_cursor.h                  ob_log_cursor.cpp                    \
  ob_log_dir_scanner.h             ob_log_dir_scanner.cpp               \
  ob_log_entry.h                   ob_log_entry.cpp                     \
  ob_log_generator.h               ob_log_generator.cpp                 \
  ob_log_reader.h                  ob_log_reader.cpp                    \
  ob_log_replay_runnable.h         ob_log_replay_runnable.cpp           \
  ob_log_writer.h                  ob_log_writer.cpp                    \
  ob_log_writer2.h                 ob_log_writer2.cpp                   \
  ob_log_data_writer.h                 ob_log_data_writer.cpp                   \
  ob_login_mgr.h                   ob_login_mgr.cpp                     \
  ob_lrucache.h                                                         \
  ob_malloc.h                      ob_malloc.cpp                        \
  ob_memory_pool.h                 ob_memory_pool.cpp                   \
  ob_merger.h                      ob_merger.cpp                        \
  ob_meta_cache.h                  ob_meta_cache.cpp                    \
  ob_mod_define.h                  ob_mod_define.cpp                    \
  ob_ms_list.h                     ob_ms_list.cpp                       \
  ob_mutator.h                     ob_mutator.cpp                       \
  ob_mutator_helper.h              ob_mutator_helper.cpp                \
  ob_mutex_task.h                  ob_mutex_task.cpp                    \
  ob_new_scanner.h                 ob_new_scanner.cpp                   \
  ob_new_scanner_helper.h          ob_new_scanner_helper.cpp            \
  ob_number.h                      ob_number.cpp                        \
  ob_obi_role.h                    ob_obi_role.cpp                      \
  ob_obj_cast.h                    ob_obj_cast.cpp                      \
  ob_object.h                      ob_object.cpp                        \
  ob_obj_type.h                    ob_obj_type.cpp                      \
  ob_operate_result.h                                                   \
  ob_packet.h                      ob_packet.cpp                        \
  ob_packet_factory.h                                                   \
  ob_packet_queue.h                ob_packet_queue.cpp                  \
  ob_packet_queue_handler.h                                             \
  ob_packet_queue_thread.h         ob_packet_queue_thread.cpp           \
  ob_perm_components.h             ob_perm_components.cpp               \
  ob_pointer_array.h                                                    \
  ob_pool.h                        ob_pool.ipp                          \
  ob_pooled_allocator.h                          \
  ob_postfix_expression.h          ob_postfix_expression.cpp            \
  ob_privilege.h                   ob_privilege.cpp                     \
  ob_privilege_manager.h           ob_privilege_manager.cpp             \
  ob_privilege_type.h              ob_privilege_type.cpp                \
  ob_probability_random.h          ob_probability_random.cpp            \
  ob_range.h                       ob_range.cpp                         \
  ob_range2.h                      ob_range2.cpp                        \
  ob_raw_row.h                     ob_raw_row.cpp                       \
  ob_read_common_data.h                                                 \
  ob_record_header.h               ob_record_header.cpp                 \
	ob_record_header_v2.h            ob_record_header_v2.cpp              \
  ob_regex.h                       ob_regex.cpp                         \
  ob_repeated_log_reader.h         ob_repeated_log_reader.cpp           \
  ob_result.h                                                           \
  ob_return_operator.h             ob_return_operator.cpp               \
  ob_ring_buffer.h                 ob_ring_buffer.cpp                   \
  ob_role_mgr.h                                                         \
  ob_row.h                         ob_row.cpp                           \
  ob_row_compaction.h              ob_row_compaction.cpp                \
  ob_row_desc.h                    ob_row_desc.cpp                      \
  ob_row_desc_ext.h                ob_row_desc_ext.cpp                  \
  ob_row_fuse.h                    ob_row_fuse.cpp                      \
  ob_row_iterator.h                                                     \
  ob_row_store.h                   ob_row_store.cpp                     \
  ob_row_util.h                    ob_row_util.cpp                      \
  ob_rowkey.h                      ob_rowkey.cpp                        \
  ob_rowkey_helper.h               ob_rowkey_helper.cpp                 \
  ob_rs_ups_message.h              ob_rs_ups_message.cpp                \
  ob_scan_param.h                  ob_scan_param.cpp                    \
  ob_scanner.h                     ob_scanner.cpp                       \
  ob_schema.h                      ob_schema.cpp                        \
  ob_schema_helper.h               ob_schema_helper.cpp                 \
  ob_schema_manager.h              ob_schema_manager.cpp                \
  ob_schema_service.h              ob_schema_service.cpp                \
  ob_schema_service_impl.h         ob_schema_service_impl.cpp           \
  ob_schema_table.h                ob_schema_table.cpp                  \
  ob_seq_queue.h                   ob_seq_queue.cpp                     \
  ob_server.h                      ob_server.cpp                        \
  ob_server_ext.h                  ob_server_ext.cpp                    \
  ob_server_getter.h               ob_server_getter.cpp                 \
  ob_session_mgr.h                 ob_session_mgr.cpp                   \
  ob_shadow_server.h               ob_shadow_server.cpp                 \
  ob_simple_condition.h            ob_simple_condition.cpp              \
  ob_simple_filter.h               ob_simple_filter.cpp                 \
  ob_simple_right_join_cell.h                                           \
  ob_simple_tpl.h                                                       \
  ob_single_log_reader.h           ob_single_log_reader.cpp             \
  ob_single_server.h               ob_single_server.cpp                 \
  ob_slab.h                        ob_slab.cpp                          \
  ob_slave_mgr.h                   ob_slave_mgr.cpp                     \
  ob_spin_lock.h                                                        \
  ob_recursive_mutex.h                                                  \
  ob_spin_rwlock.h                                                      \
  ob_spop_spush_queue.h            ob_spop_spush_queue.cpp              \
  ob_sql_ups_rpc_proxy.h                                                \
  ob_tsi_utils.h               ob_tsi_utils.cpp\
  ob_stack_allocator.h             ob_stack_allocator.cpp               \
  ob_queued_lock.h               \
  ob_wait_queue.h \
  ob_ack_queue.h             ob_ack_queue.cpp                           \
  ob_ticket_queue.h             ob_ticket_queue.cpp                     \
  ob_statistics.h                  ob_statistics.cpp                    \
  ob_common_stat.h                 ob_common_stat.cpp                   \
  ob_string.h                                                           \
  ob_string_buf.h                  ob_string_buf.ipp                    \
  ob_string_search.h               ob_string_search.cpp                 \
  ob_strings.h                     ob_strings.cpp                       \
  ob_switch.h                                                           \
  ob_table_id_name.h               ob_table_id_name.cpp                 \
  ob_tablet_info.h                 ob_tablet_info.cpp                   \
  ob_task.h                                                             \
  ob_tbnet_callback.h              ob_tbnet_callback.cpp                \
  ob_tc_counter.h                                                       \
  ob_thread_mempool.h              ob_thread_mempool.cpp                \
  ob_thread_objpool.h                                                   \
  ob_thread_store.h                                                     \
  ob_timer.h                       ob_timer.cpp                         \
	ob_tl_store.h                                                         \
  ob_token.h                       ob_token.cpp                         \
  ob_trace_log.h                   ob_trace_log.cpp                     \
  ob_tsi_factory.h                 ob_tsi_factory.cpp                   \
  ob_typed_queue.h                                                      \
  ob_ups_info.h                    ob_ups_info.cpp                      \
  ob_ups_row.h                     ob_ups_row.cpp                       \
  ob_ups_row_util.h                ob_ups_row_util.cpp                  \
  ob_ups_rpc_proxy.h                                                    \
  ob_vector.h                      ob_vector.ipp                        \
  page_arena.h                                                          \
  priority_packet_queue_thread.h   priority_packet_queue_thread.cpp     \
  ob_priority_scheduler.h                                               \
  qlock.h                                                               \
  serialization.h                                                       \
  thread_buffer.h                  thread_buffer.cpp                    \
  utility.h                        utility.cpp                          \
  ob_rpc_stub.h                    ob_rpc_stub.cpp                      \
  ob_general_rpc_stub.h            ob_general_rpc_stub.cpp              \
  ob_general_rpc_proxy.h           ob_general_rpc_proxy.cpp             \
  ob_tsi_block_allocator.h         ob_tsi_block_allocator.cpp           \
  ob_pcap.h         ob_pcap.cpp           \
  ob_iterator_adaptor.h            ob_iterator_adaptor.cpp              \
  ob_transaction.h                 ob_transaction.cpp                   \
  nb_accessor/ob_nb_accessor.h     nb_accessor/ob_nb_accessor.cpp       \
  nb_accessor/nb_scan_cond.h       nb_accessor/nb_scan_cond.cpp         \
  nb_accessor/nb_query_res.h       nb_accessor/nb_query_res.cpp         \
	ob_profile_log.h								 ob_profile_log.cpp										\
	ob_trace_id.h										 ob_trace_id.cpp                      \
	ob_profile_type.h                                                     \
  nb_accessor/nb_table_row.h       nb_accessor/nb_table_row.cpp         \
  ob_allocator.h                                                        \
  ob_schema_macro_define.h                                              \
  ob_cellinfo_processor.h                                               \
  ob_chunk_server_item.h                                                \
	ob_trigger_msg.h                 ob_trigger_msg.cpp                   \
	ob_trigger_event.h                                                    \
  ob_trigger_event_util.h          ob_trigger_event_util.cpp            \
  ob_inner_table_operator.h        ob_inner_table_operator.cpp          \
  ob_cluster_server.h\
  ob_rpc_macros.h\
	ob_encrypted_helper.h	ob_encrypted_helper.cpp\
  ob_se_array.h \
  ob_server_config.h                 ob_server_config.cpp \
  ob_system_config.h                 ob_system_config.cpp	\
  ob_system_config_key.h             ob_system_config_key.cpp	\
  ob_system_config_value.h					\
  ob_config_manager.h                ob_config_manager.cpp      \
  ob_config.h                        ob_config.cpp              \
  ob_config_helper.h                 ob_config_helper.cpp       \
	ob_version.h											 ob_version.cpp\
	ob_profile_fill_log.h              ob_profile_fill_log.cpp\
	ob_request_profile_data.h\
  ob_queue_thread.h                  ob_queue_thread.cpp\
	ob_libeasy_mem_pool.h              ob_libeasy_mem_pool.cpp\
	ob_balance_filter.h								 ob_balance_filter.cpp\
	ob_adapter_allocator.h\
	ob_reload_config.h\
	ob_obj_pool.h \
	ob_data_source_desc.h              ob_data_source_desc.cpp \
         ob_tc_factory.h\
	ob_fifo_stream.h									 ob_fifo_stream.cpp \
	ob_multi_wakeup.h									 ob_multi_wakeup.cpp \
	ob_disk_checker.h									 ob_disk_checker.cpp\
	ob_global_factory.h\
  ob_errno.h ob_errno.cpp\
  ob_custom_allocator.h  ob_custom_allocator.cpp\
	Ob_Decimal.h	Ob_Decimal.cpp\
	ob_rs_rs_message.h	ob_rs_rs_message.cpp \
	ob_rs_address_mgr.h	ob_rs_address_mgr.cpp \
  ttmathint.h	ttmathmisc.h	ttmathuint.h	ttmathuint_noasm.h	ttmathuint_x86_64.h	ttmathuint_x86.h ttmathtypes.h	ob_column_checksum.h\
  ob_tablet_histogram.h		ob_tablet_histogram.cpp		ob_tablet_histogram_meta.h	ob_tablet_histogram_meta.cpp\
  ob_tablet_histogram_report_info.h ob_tablet_histogram_report_info.cpp	ob_black_list.h	ob_black_list.cpp


location_sources=                                                                             \
  location/ob_ms_cache_table.h    location/ob_btree_map.h                                     \
  location/ob_tablet_location_list.h   location/ob_tablet_location_list.cpp                   \
  location/ob_tablet_location_cache_proxy.h   location/ob_tablet_location_cache_proxy.cpp     \
  location/ob_tablet_location_range_iterator.h location/ob_tablet_location_range_iterator.cpp \
  location/ob_tablet_location_cache.h  location/ob_tablet_location_cache.cpp


libcommon_a_SOURCES = ${common_sources} ${location_sources}

#bin_PROGRAMS=test
#test_SOURCES=test.cpp
#test_LDFLAGS=libcommon.a -lpthread -lc -lm  -lrt -lcrypt -lssl -laio #-pg
SUBDIRS=btree compress roottable timezone
DIST_SUBDIRS=btree compress roottable timezone

EXTRA_DIST= ob_define.h
pkgdata_data_DATA= ob_define.h
pkgdata_datadir=${prefix}/include

clean-local:
	-rm -f *.gcov *.gcno *.gcda
