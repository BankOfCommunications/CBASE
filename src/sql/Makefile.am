AM_CPPFLAGS := $(AM_CPPFLAGS) -I${TBLIB_ROOT}/include/tbsys \
             -I${EASY_ROOT}/include/easy   \
             -I${top_srcdir}/include       \
             -I${top_srcdir}/src

LDADD = ${top_srcdir}/src/mergeserver/libmergeserver.a \
        ${top_srcdir}/src/sstable/libsstable.a         \
        ${EASY_LIB_PATH}/libeasy.a                     \
        ${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS=-lpthread -lc -lm  -lrt -lcrypt -lssl -lboost_regex #-pg
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

#sql_parser.tab.c sql_parser.tab.h type_name.c: sql_parser.y
#        tools/bison -d $<
#        tools/gen_type_name.sh ob_item_type.h > type_name.c

#sql_parser.lex.c sql_parser.lex.h : sql_parser.l sql_parser.tab.h
#        tools/flex -osql_parser.lex.c $<

sql_parser_sources=                                    \
  build_plan.h                build_plan.cpp           \
  dml_build_plan.h            dml_build_plan.cpp       \
  ob_alter_table_stmt.h       ob_alter_table_stmt.cpp  \
  ob_alter_sys_cnf_stmt.h     ob_alter_sys_cnf_stmt.cpp \
  ob_basic_stmt.h                                      \
  ob_column_def.h                                      \
  ob_create_table_stmt.h      ob_create_table_stmt.cpp \
  ob_deallocate_stmt.h        ob_deallocate_stmt.cpp   \
  ob_delete_stmt.h            ob_delete_stmt.cpp       \
  ob_drop_table_stmt.h        ob_drop_table_stmt.cpp   \
  ob_truncate_table_stmt.h    ob_truncate_table_stmt.cpp   \
  ob_dual_table_scan.h ob_dual_table_scan.cpp          \
  ob_execute_stmt.h           ob_execute_stmt.cpp      \
  ob_explain_stmt.h           ob_explain_stmt.cpp      \
  ob_insert_stmt.h            ob_insert_stmt.cpp       \
  ob_item_type.h                                       \
  ob_item_type_str.h                                   \
  ob_logical_plan.h           ob_logical_plan.cpp      \
  ob_multi_logic_plan.h       ob_multi_logic_plan.cpp  \
  ob_multi_phy_plan.h         ob_multi_phy_plan.cpp    \
  ob_physical_plan.h          ob_physical_plan.cpp     \
  ob_prepare_stmt.h           ob_prepare_stmt.cpp      \
  ob_raw_expr.h               ob_raw_expr.cpp          \
  ob_schema_checker.h         ob_schema_checker.cpp    \
  ob_select_stmt.h            ob_select_stmt.cpp       \
  ob_sequence_stmt.h           ob_sequence_stmt.cpp     \
  ob_show_schema_manager.h    ob_show_schema_manager.cpp \
  ob_show_stmt.h              ob_show_stmt.cpp         \
  ob_stmt.h                   ob_stmt.cpp              \
  ob_sys_params_mgr.h         ob_sys_params_mgr.cpp    \
  ob_transformer.h            ob_transformer.cpp       \
  ob_update_stmt.h            ob_update_stmt.cpp       \
  ob_variable_set_stmt.h      ob_variable_set_stmt.cpp \
  ob_kill_stmt.h              ob_kill_stmt.cpp         \
  ob_semi_join.h	      ob_semi_join.cpp	       \
  parse_malloc.h              parse_malloc.cpp         \
  parse_node.h                parse_node.c             \
  sql_parser.lex.c         	\
  sql_parser.lex.h            sql_parser.tab.c         \
  sql_parser.tab.h            type_name.c\
  priv_build_plan.h priv_build_plan.cpp\
  ob_drop_user_stmt.h ob_drop_user_stmt.cpp\
  ob_set_password_stmt.h ob_set_password_stmt.cpp\
  ob_rename_user_stmt.h ob_rename_user_stmt.cpp\
  ob_lock_user_stmt.h ob_lock_user_stmt.cpp\
  ob_grant_stmt.h ob_grant_stmt.cpp\
  ob_revoke_stmt.h ob_revoke_stmt.cpp\
  ob_start_trans_stmt.h\
  ob_end_trans_stmt.h\
  ob_non_reserved_keywords.h  ob_non_reserved_keywords.c \
	ob_priv_executor.h ob_priv_executor.cpp\
	ob_change_obi_stmt.h ob_change_obi_stmt.cpp	ob_create_db_stmt.h	ob_create_db_stmt.cpp	ob_use_db_stmt.h	ob_use_db_stmt.cpp	ob_drop_database_stmt.h	ob_drop_database_stmt.cpp

operators_sources=                                                       \
  ob_add_project.h                   ob_add_project.cpp                  \
  ob_aggregate_function.h            ob_aggregate_function.cpp           \
  ob_alter_sys_cnf.h                 ob_alter_sys_cnf.cpp                \
  ob_alter_table.h                   ob_alter_table.cpp                  \
  ob_column_group_scanner.h          ob_column_group_scanner.cpp         \
  ob_create_table.h                  ob_create_table.cpp                 \
  ob_create_user_stmt.h ob_create_user_stmt.cpp                          \
  ob_deallocate.h                    ob_deallocate.cpp                   \
  ob_delete.h                        ob_delete.cpp                       \
  ob_double_children_phy_operator.h  ob_double_children_phy_operator.cpp \
  ob_drop_table.h                    ob_drop_table.cpp                   \
  ob_truncate_table.h                ob_truncate_table.cpp               \
  ob_execute.h                       ob_execute.cpp                      \
  ob_explain.h                       ob_explain.cpp                      \
  ob_filter.h                        ob_filter.cpp                       \
  ob_groupby.h                       ob_groupby.cpp                      \
  ob_in_memory_sort.h                ob_in_memory_sort.cpp               \
  ob_insert.h                        ob_insert.cpp                       \
  ob_join.h                          ob_join.cpp                         \
  ob_limit.h                         ob_limit.cpp                        \
  ob_lock_v0.h                       ob_lock_v0.cpp                       \
  ob_merge_distinct.h                ob_merge_distinct.cpp               \
  ob_merge_except.h                  ob_merge_except.cpp                 \
  ob_merge_groupby.h                 ob_merge_groupby.cpp                \
  ob_merge_intersect.h               ob_merge_intersect.cpp              \
  ob_merge_join.h                    ob_merge_join.cpp                   \
  ob_bloomfilter_join.h              ob_bloomfilter_join.cpp             \
  ob_merge_sort.h                    ob_merge_sort.cpp                   \
  ob_merge_union.h                   ob_merge_union.cpp                  \
  ob_multi_cg_scanner.h              ob_multi_cg_scanner.cpp             \
  ob_no_children_phy_operator.h                                          \
  ob_phy_operator.h                  ob_phy_operator.cpp                 \
  ob_postfix_expression.h            ob_postfix_expression.cpp           \
  ob_prepare.h                       ob_prepare.cpp                      \
  ob_project.h                       ob_project.cpp                      \
  ob_rename.h                        ob_rename.cpp                       \
  ob_result_set.h                    ob_result_set.cpp                   \
  ob_rowkey_phy_operator.h           ob_rowkey_phy_operator.cpp          \
  ob_rpc_scan.h                      ob_rpc_scan.cpp                     \
  ob_run_file.h                      ob_run_file.cpp                     \
  ob_scalar_aggregate.h              ob_scalar_aggregate.cpp             \
  ob_set_operator.h                  ob_set_operator.cpp                 \
  ob_sequence_define.h                                                   \
  ob_sequence.h                      ob_sequence.cpp                     \
  ob_sequence_delete.h               ob_sequence_delete.cpp              \
  ob_sequence_insert.h               ob_sequence_insert.cpp              \
  ob_sequence_update.h               ob_sequence_update.cpp              \
  ob_sequence_select.h               ob_sequence_select.cpp              \
  ob_single_child_phy_operator.h     ob_single_child_phy_operator.cpp    \
  ob_sort.h                          ob_sort.cpp                         \
  ob_sort_helper.h                                                       \
  ob_sql.h                           ob_sql.cpp                          \
  ob_sql_context.h                                                       \
  ob_sql_expression.h                ob_sql_expression.cpp               \
  ob_sql_read_param.h                ob_sql_read_param.cpp               \
  ob_sql_scan_param.h                ob_sql_scan_param.cpp               \
  ob_sql_get_param.h                 ob_sql_get_param.cpp                \
  ob_sql_session_info.h              ob_sql_session_info.cpp             \
  ob_sql_session_mgr.h               ob_sql_session_mgr.cpp              \
  ob_sql_session_info_traversal.h    ob_sql_session_info_traversal.cpp   \
  ob_sql_session_info_callback.h                                         \
  ob_sstable_block_scanner.h         ob_sstable_block_scanner.cpp        \
  ob_sstable_scan.h                  ob_sstable_scan.cpp                 \
  ob_sstable_scanner.h               ob_sstable_scanner.cpp              \
  ob_table_mem_scan.h                ob_table_mem_scan.cpp               \
  ob_table_rename.h                  ob_table_rename.cpp                 \
  ob_table_rpc_scan.h                ob_table_rpc_scan.cpp               \
  ob_table_scan.h                                                        \
  ob_tablet_fuse.h                                                       \
  ob_tablet_join.h                   ob_tablet_join.cpp                  \
  ob_tablet_cache_join.h             ob_tablet_cache_join.cpp            \
  ob_tablet_direct_join.h            ob_tablet_direct_join.cpp           \
  ob_tablet_scan.h                   ob_tablet_scan.cpp                  \
  ob_ups_multi_get.h                 ob_ups_multi_get.cpp                \
  ob_ups_scan.h                      ob_ups_scan.cpp                     \
  ob_values.h                        ob_values.cpp                       \
  ob_variable_set.h                  ob_variable_set.cpp                 \
  ob_ups_modify.h                    ob_ups_modify.cpp                   \
  ob_inc_scan.h                      ob_inc_scan.cpp                     \
  ob_update.h                        ob_update.cpp                       \
  ob_phy_operator_type.h             ob_phy_operator_type.cpp            \
  ob_mem_sstable_scan.h              ob_mem_sstable_scan.cpp             \
  ob_multiple_merge.h                ob_multiple_merge.cpp               \
  ob_inc_scan.h                      ob_inc_scan.cpp                     \
  ob_insert_dbsem_filter.h ob_insert_dbsem_filter.cpp                    \
  ob_index_trigger.h		         ob_index_trigger.cpp                \
  ob_delete_index.h			ob_delete_index.cpp		\
  ob_cs_create_plan.h                ob_cs_create_plan.cpp               \
  ob_phy_operator_factory.h          ob_phy_operator_factory.cpp         \
  ob_husk_filter.h	              \
  ob_lock_filter.h                   ob_lock_filter.cpp                  \
  ob_ups_executor.h                  ob_ups_executor.cpp\
  ob_start_trans.h                   ob_start_trans.cpp \
  ob_last_rowkey_interface.h \
  ob_end_trans.h                     ob_end_trans.cpp\
  ob_expr_values.h                   ob_expr_values.cpp \
  ob_sstable_get.h                   ob_sstable_get.cpp \
  ob_tablet_get_fuse.h               ob_tablet_get_fuse.cpp \
  ob_tablet_scan_fuse.h              ob_tablet_scan_fuse.cpp \
  ob_tablet_read.h                   ob_tablet_read.cpp \
  ob_tablet_get.h                    ob_tablet_get.cpp \
  ob_sql_read_param.h                ob_sql_read_param.cpp \
  ob_multiple_scan_merge.h           ob_multiple_scan_merge.cpp \
  ob_multiple_get_merge.h            ob_multiple_get_merge.cpp \
  ob_empty_row_filter.h              ob_empty_row_filter.cpp \
  ob_sql_read_strategy.h             ob_sql_read_strategy.cpp \
  ob_tablet_join_cache.h             ob_tablet_join_cache.cpp \
  ob_direct_trigger_event_util.h     ob_direct_trigger_event_util.cpp \
  ob_multi_children_phy_operator.h   ob_multi_children_phy_operator.cpp \
  ob_row_count.h                     ob_row_count.cpp \
  ob_when_filter.h                   ob_when_filter.cpp \
  ob_get_cur_time_phy_operator.h     ob_get_cur_time_phy_operator.cpp \
  ob_kill_session.h                  ob_kill_session.cpp \
  ob_sql_id_mgr.h                    ob_sql_id_mgr.cpp \
	ob_change_obi.h                    ob_change_obi.cpp\
  ob_sql_query_cache.h               ob_sql_query_cache.cpp \
  ob_sql_id_mgr.h       ob_sql_id_mgr.cpp \
  ob_ps_store_item.h    ob_ps_store_item.cpp \
  ob_ps_store.h         ob_ps_store.cpp      \
  ob_ps_store_item_callback.h \
	ob_sql_character_set.h             ob_sql_character_set.cpp \
	ob_multi_bind.h             ob_multi_bind.cpp	ob_create_index_stmt.h	ob_create_index_stmt.cpp	ob_index_trigger_upd.h	ob_index_trigger_upd.cpp	ob_drop_index.h	ob_drop_index.cpp	ob_drop_index_stmt.h	ob_drop_index_stmt.cpp \
  ob_index_trigger_rep.h  ob_index_trigger_rep.cpp\
  ob_bind_values.h		ob_bind_values.cpp  \
  ob_iud_loop_control.h	ob_iud_loop_control.cpp \
  ob_fill_values.h              ob_fill_values.cpp        \
  ob_update_dbsem_filter.h      ob_update_dbsem_filter.cpp



misc_sources =							\
  ob_type_convertor.h						\
  ob_ups_result.h                    ob_ups_result.cpp		\
  ob_sql_result_set.h                ob_sql_result_set.cpp	\
  ob_sys_params_mgr.h                ob_sys_params_mgr.cpp      \
  ob_affected_rows.h                                            \
  ob_duplicate_indicator.h\
  ob_sql_config_provider.h

noinst_LIBRARIES = libsql.a

libsql_a_SOURCES = ${sql_parser_sources} ${operators_sources} ${misc_sources}

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk

clean-local:
	-rm -f *.gcov *.gcno *.gcda
