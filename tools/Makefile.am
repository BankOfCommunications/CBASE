AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${EASY_ROOT}/include/easy \
			  -I${EASY_ROOT}/include/common \
				-I${top_srcdir}/include \
			  -I${top_srcdir}/tests/updateserver \
			  -I${top_srcdir}/src \
			  -fPIC

LIBTOOLFLAGS=--preserve-dup-deps

LDADD =$(top_builddir)/src/updateserver/libupdateserver.a \
		$(top_builddir)/src/rootserver/librootserver.a \
		$(top_builddir)/src/chunkserver/libchunkserver.a  \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/sql/libsql.a  \
		$(top_builddir)/src/compactsstable/libcompactsstable.a  \
		$(top_builddir)/src/compactsstablev2/libcompactsstablev2.a  \
		$(top_builddir)/src/mergeserver/libmergeserver.a \
		$(top_builddir)/src/obmysql/libobmysql.a \
                $(top_builddir)/src/sql/libsql.a  \
                $(top_builddir)/src/mergeserver/libmergeserver.a \
		$(top_builddir)/src/sstable/libsstable.a \
		$(top_builddir)/src/chunkserver/libchunkserver.a  \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/compress/libcomp.a \
                $(top_builddir)/src/common/roottable/libroottable.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${EASY_LIB_PATH}/libeasy.a \
		${TBLIB_ROOT}/lib/libtbsys.a -lcrypt

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lreadline -lncurses -lcrypt -lssl -lboost_regex ${GCOV_LIB}
CXXFLAGS += -D_BTREE_ENGINE_
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

#bin_PROGRAMS = sstable_checker test_client mergemeta gen_sstable cs_admin merge_meta_new cs_info_reader ups_admin gen_meta databuilder dumpsst gen_data_test gen_data_testV3 log_reader
bin_PROGRAMS = sstable_checker gen_sstable gen_meta gen_data_testV3 log_reader cs_admin backup_client dumpsst ups_admin convert_idx_file search_sstable oberror

sstable_checker_SOURCES = ob_sstable_checker.cpp
test_client_SOURCES = test_client.cpp  $(top_builddir)/src/updateserver/ob_ups_stat.cpp
databuilder_SOURCES = ob_databuilder.cpp feak_disk_path.cpp
#databuilder_CPPFLAGS= -DORDER_SERIALIZE ${AM_CPPFLAGS} #-DBUILDER_DEBUG
#test_manager_SOURCES = test_manager.cpp ob_disk_path.cpp
#mergemeta_SOURCES = ob_merge_meta.cpp ob_tablet_meta.cpp
gen_sstable_SOURCES=gen_sstable.cpp feak_disk_path.cpp
gen_data_test_SOURCES=gen_data_test.cpp ob_disk_path.cpp
gen_data_testV3_SOURCES=gen_data_testV3.cpp ob_disk_path.cpp
#cs_press_SOURCES = base_client.cpp cs_press.cpp client_rpc.cpp common_func.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
cs_admin_SOURCES = cs_admin.cpp client_rpc.cpp common_func.cpp stats.cpp ob_server_stats.cpp ob_cluster_stats.cpp ob_disk_path.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
backup_client_SOURCES = backup_client.cpp client_rpc.cpp common_func.cpp stats.cpp ob_server_stats.cpp ob_cluster_stats.cpp ob_disk_path.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
dumpsst_SOURCES = dumpsst.cpp feak_disk_path.cpp common_func.cpp
gen_data_test_CPPFLAGS= -DGEN_SSTABLE_DEBUG ${AM_CPPFLAGS}
merge_meta_new_SOURCES = merge_meta_new.cpp ob_disk_path.cpp

log_reader_SOURCES = log_reader.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
#add by Yukun
log_agent_SOURCES = log_agent.cpp sql_builder.cpp

ups_admin_SOURCES = ups_admin.cpp $(top_builddir)/tests/updateserver/test_utils.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
gen_meta_SOURCES =  gen_meta.cpp common_func.cpp feak_disk_path.cpp
log_locator_SOURCES = log_locator.cpp
#authority_admin_SOURCES = ob_authority_manager_main.cpp ob_authority_manager.cpp
convert_idx_file_SOURCES = convert_idx_file.cpp feak_disk_path.cpp
search_sstable_SOURCES = search_sstable.cpp feak_disk_path.cpp common_func.cpp
oberror_SOURCES=oberror.cpp

EXTRA_DIST = \
			 data_syntax.h \
			 ob_databuilder.h \
			 ob_sstable_checker.h \
			 ob_tablet_meta.h \
			 gen_sstable.h \
			 cs_press.h \
			 common_func.h \
			 sql_builder.h \
			 stats.h \
			 client_rpc.h \
			 ob_cluster_stats.h \
			 ob_server_stats.h \
			 cs_admin.h \
			 backup_client.h \
			 gen_data_test.h \
			 gen_data_testV3.h \
			 search_sstable.h \
			 dumpsst.h \
			 oceanbase.sh \
			 sysctl.conf \
			 cs_init.sh \
			 snmpd.conf \
			 ups_init.sh

dist_cases_DATA=deploy/mysql_test/t/create.test deploy/mysql_test/t/count_distinct.test deploy/mysql_test/t/join_basic.test deploy/mysql_test/t/group_by_1.test deploy/mysql_test/t/sq_from.test deploy/mysql_test/t/show.test deploy/mysql_test/t/ps_complex.test deploy/mysql_test/r/create.result deploy/mysql_test/r/count_distinct.result deploy/mysql_test/r/join_basic.result deploy/mysql_test/r/group_by_1.result deploy/mysql_test/r/sq_from.result deploy/mysql_test/r/show.result deploy/mysql_test/r/ps_complex.result
casesdir=${prefix}/tests

dist_conf_DATA=sysctl.conf snmpd.conf
confdir=${prefix}/etc/

if WITH_TEST_CASE
NEWSQLTEST:=newsqltest
endif
if WITH_MYSQLTEST
MYSQLTEST:=mysqltest
endif

SUBDIRS=benchmark sstable_builder syschecker $(NEWSQLTEST) mixed_test log_tool compactsstablev2 getquerytest dump $(MYSQLTEST) liboblog

clean-local:
	-rm -f *.gcov *.gcno *.gcda
