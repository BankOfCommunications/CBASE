AM_LIBTOOLFLAGS=--preserve-dup-deps

AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			 	-I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/common/hash

LIBTOOLFLAGS=--preserve-dup-deps

LDADD = \
		${top_builddir}/src/sql/libsql.a \
		${top_builddir}/src/mergeserver/libmergeserver.a \
		${top_builddir}/src/chunkserver/libchunkserver.a \
    $(top_builddir)/src/common/roottable/libroottable.a \
		$(top_builddir)/src/obmysql/libobmysql.a \
		${top_builddir}/src/compactsstablev2/libcompactsstablev2.a \
		${top_builddir}/src/compactsstable/libcompactsstable.a \
		${top_builddir}/src/sstable/libsstable.a \
    ${top_builddir}/src/updateserver/libupdateserver.a \
    ${top_builddir}/src/sstable/libsstable.a \
		${top_builddir}/src/common/libcommon.a \
		${top_builddir}/src/sql/libsql.a \
    ${top_builddir}/src/mergeserver/libmergeserver.a \
		${top_builddir}/src/common/compress/libcomp.a \
		${top_builddir}/src/common/libcommon.a \
		${top_builddir}/src/sql/libsql.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${EASY_LIB_PATH}/libeasy.a \
		${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS=-lpthread -lc -lm -lrt -lgmock -lgtest -lboost_regex ${GCOV_LIB} -lcrypt -lreadline -lncurses -laio -lssl
CXXFLAGS+= -g -DCOMPATIBLE
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov -fprofile-arcs
endif

pub_source= \
          ob_fake_table.h ob_fake_table.cpp \
          test_utility.h test_utility.cpp \
					test_helper.h test_helper.cpp \
					ob_file_table.h ob_file_table.cpp \
					ob_ups_file_table.h ob_ups_file_table.cpp \
					ob_fake_ups_scan.h ob_fake_ups_scan.cpp \
					ob_fake_ups_multi_get.h ob_fake_ups_multi_get.cpp \
					ob_fake_sql_ups_rpc_proxy.h ob_fake_sql_ups_rpc_proxy.cpp \
					ob_fake_sql_ups_rpc_proxy2.h ob_fake_sql_ups_rpc_proxy2.cpp \
					ob_fake_sstable_scan.h ob_fake_sstable_scan.cpp \
					test_sstable_stat.cpp \
					test_disk_path.cpp

check_programs= \
            ob_row_desc_test \
            ob_run_file_test \
            ob_in_memory_sort_test \
            ob_sort_test \
            ob_postfix_expression_test \
            ob_sql_expression_test \
            ob_project_test \
            ob_filter_test \
            ob_limit_test \
            ob_aggregate_function_test\
            ob_phy_operators_test \
            ob_file_table_test \
            sql_logical_plan_test \
            sql_physical_plan_test\
            ob_add_project_test \
            ob_single_table_sql_test\
            ob_result_set_test \
			test_sstable_block_scanner \
			test_sstable_scan \
			ob_union_test\
			ob_fake_ups_multi_get_test \
			ob_fake_sql_ups_rpc_proxy_test2 \
			ob_ups_scan_test \
			ob_ups_multi_get_test \
			ob_tablet_fuse_test \
			ob_physical_plan_test \
			ob_except_test

bin_PROGRAMS= ${check_programs}\
            sqlparser \
            sqlplan

#
#      test_insert_dbsem_filter
#      ob_multiple_merge_test
#      ob_tablet_scan_test
#			ob_intersect_test
#      test_ups_result
#ob_tablet_join_test
#test_ob_ps_store
#

ob_row_desc_test_SOURCES=ob_row_desc_test.cpp
ob_run_file_test_SOURCES=ob_run_file_test.cpp ${pub_source}
ob_in_memory_sort_test_SOURCES=ob_in_memory_sort_test.cpp ${pub_source}
ob_sort_test_SOURCES=ob_sort_test.cpp ${pub_source}
ob_postfix_expression_test_SOURCES=ob_postfix_expression_test.cpp
ob_sql_expression_test_SOURCES=ob_sql_expression_test.cpp
ob_project_test_SOURCES=ob_project_test.cpp ${pub_source}
ob_filter_test_SOURCES=ob_filter_test.cpp ${pub_source}
ob_limit_test_SOURCES=ob_limit_test.cpp ${pub_source}
ob_aggregate_function_test_SOURCES=ob_aggregate_function_test.cpp ${pub_source}
ob_phy_operators_test_SOURCES=ob_phy_operators_test.cpp ${pub_source}
ob_file_table_test_SOURCES=ob_file_table_test.cpp ${pub_source}
ob_add_project_test_SOURCES=ob_add_project_test.cpp ${pub_source}
sqlparser_SOURCES = sql_parser_main.cpp
sqlplan_SOURCES = sql_plan_main.cpp
sql_logical_plan_test_SOURCES = test_sql_parser.cpp
sql_physical_plan_test_SOURCES = test_gen_phy_plan.cpp
ob_single_table_sql_test_SOURCES = ob_single_table_sql_test.cpp ${pub_source}
#ob_multiple_merge_join_test_SOURCES = ob_multiple_merge_join_test.cpp ${pub_source}
ob_result_set_test_SOURCES = ob_result_set_test.cpp
test_sstable_block_scanner_SOURCES=test_sstable_block_scanner.cpp test_helper.cpp test_sstable_stat.cpp test_disk_path.cpp
test_sstable_scan_SOURCES=test_sstable_scan.cpp test_helper.cpp test_sstable_stat.cpp test_disk_path.cpp
ob_union_test_SOURCES = ob_union_test.cpp $(pub_source)
ob_fake_ups_multi_get_test_SOURCES =  ob_fake_ups_multi_get_test.cpp ${pub_source}
ob_fake_sql_ups_rpc_proxy_test2_SOURCES = ob_fake_sql_ups_rpc_proxy_test2.cpp ${pub_source}
ob_ups_scan_test_SOURCES = ob_ups_scan_test.cpp ${pub_source}
ob_ups_multi_get_test_SOURCES = ob_ups_multi_get_test.cpp ${pub_source}
ob_tablet_fuse_test_SOURCES = ob_tablet_fuse_test.cpp ${pub_source}
#ob_tablet_join_test_SOURCES = ob_tablet_join_test.cpp ${pub_source}
#ob_tablet_scan_test_SOURCES = ob_tablet_scan_test.cpp ${pub_source}
ob_except_test_SOURCES = ob_except_test.cpp ${pub_source}
#ob_multiple_merge_test_SOURCES = ob_multiple_merge_test.cpp
#test_insert_dbsem_filter_SOURCES = test_insert_dbsem_filter.cpp
#test_ups_result_SOURCES = test_ups_result.cpp
ob_physical_plan_test_SOURCES = ob_physical_plan_test.cpp
#test_ob_ps_store_SOURCES = test_ob_ps_store.cpp
#test_ob_ps_item_allocator_SOURCES = test_ob_ps_item_allocator.cpp
#ob_mem_sstable_scan_test_SOURCES = ob_mem_sstable_scan_test.cpp

check_SCRIPTS = $(check_programs)
TESTS = $(check_SCRIPTS)

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda
