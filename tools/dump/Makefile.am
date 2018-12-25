AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
	      -I${TBLIB_ROOT}/include/tbnet \
        -I${EASY_ROOT}/include/easy\
	      -I${top_srcdir}/include \
	      -I${top_srcdir}/src \
	      -I./include

LDADD = $(top_builddir)/src/updateserver/libupdateserver.a \
	$(top_builddir)/src/rootserver/librootserver.a \
	$(top_builddir)/src/chunkserver/libchunkserver.a \
	$(top_builddir)/src/mergeserver/libmergeserver.a \
	$(top_builddir)/src/sql/libsql.a \
	$(top_builddir)/src/sstable/libsstable.a \
	$(top_builddir)/src/common/compress/libcomp.a \
	$(top_builddir)/src/common/libcommon.a \
	$(top_builddir)/src/common/timezone/libtimezone.a \
	$(top_builddir)/src/common/btree/libbtree.a \
	${TBLIB_ROOT}/lib/libtbnet.a \
	${EASY_LIB_PATH}/libeasy.a \
        ${TBLIB_ROOT}/lib/libtbsys.a -lcrypt

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -lboost_regex ${GCOV_LIB} -laio -lcrypt -lssl -L/usr/lib64/mysql -lmysqlclient
CXXFLAGS += -I${JAVA_HOME}/include -DDUMP_DEBUG

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

EXTRA_DIST= db_dumper_checkpoint.h db_file_utils.h db_queue.h \
	    db_table_info.h ob_api_util.h oceanbase_db.h \
	    db_dumper_config.h db_import.h db_record_filter.h \
	    db_thread_mgr.h ob_import_comsumer.h slice.h \
	    db_dumper.h db_log_monitor.h db_record_formator.h db_utils.h \
	    ob_import.h tokenizer.h db_dumper_mgr.h db_msg_report.h db_record.h \
	    file_appender.h ob_import_param.h db_dumper_writer.h db_parse_log.h db_record_set.h \
	    file_reader.h ob_import_producer.h \
	    scripts/obdump.sh scripts/dumper.ini scripts/ob_check_done.sh \
	    scripts/dumper.ini_master ob_data_set.h db_table_info.h \
            ob_export_param.h ob_export_extra_param.h \
            ob_export_producer.h ob_export_consumer.h \
            db_tablet_info.h ob_export_queue.h \
            file_writer.h db_row_formator.h \
            ob_export_monitor.h ob_export.template.conf \
            ob_check_producer.h db_queue_check.h ob_check.h ob_check_comsumer.h ob_check_param.h \
            ob_import_v2.h ob_import_producer_v2.h ob_import_comsumer_v2.h ob_import_param_v2.h \
            db_queue_v2.h tokenizer_v2.h ob_mysql_client_helper.h file_reader_v2.h charset.h \
			$(top_builddir)/src/common/ob_general_rpc_stub.h $(top_builddir)/src/common/ob_rpc_stub.h

obdump_SOURCES=db_record_set.cpp oceanbase_db.cpp db_table_info.cpp db_record.cpp db_dumper.cpp \
	       db_dumper_config.cpp db_parse_log.cpp main.cpp db_dumper_writer.cpp \
	       db_dumper_mgr.cpp db_log_monitor.cpp db_thread_mgr.cpp db_msg_report.cpp \
	       db_record_filter.cpp db_dumper_checkpoint.cpp db_utils.cpp db_record_formator.cpp \
	       file_appender.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp

tablet_stats_SOURCES=db_record_set.cpp oceanbase_db.cpp db_table_info.cpp db_record.cpp db_dumper.cpp \
		     db_dumper_config.cpp db_parse_log.cpp db_dumper_writer.cpp \
		     db_dumper_mgr.cpp db_log_monitor.cpp db_thread_mgr.cpp db_msg_report.cpp \
		     db_record_filter.cpp db_dumper_checkpoint.cpp db_utils.cpp db_record_formator.cpp \
		     tablet_stats.cpp file_appender.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp

ob_import_SOURCES =  tokenizer.cpp ob_import.cpp ob_import_producer.cpp \
		     db_utils.cpp file_reader.cpp oceanbase_db.cpp db_record_set.cpp \
		     ob_import_comsumer.cpp ob_import_param.cpp ob_import_main.cpp \
		     db_table_info.cpp db_record.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp \
				 file_appender.cpp db_queue.cpp $(top_builddir)/src/common/ob_general_rpc_stub.cpp $(top_builddir)/src/common/ob_rpc_stub.cpp

ob_check_SOURCES =  tokenizer.cpp ob_check.cpp ob_check_producer.cpp \
		     db_utils.cpp file_reader.cpp oceanbase_db.cpp db_record_set.cpp \
		     ob_check_comsumer.cpp ob_check_param.cpp ob_check_main.cpp \
		     db_table_info.cpp db_record.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp \
				 file_appender.cpp db_queue_check.cpp \
				 $(top_builddir)/src/common/ob_general_rpc_stub.cpp $(top_builddir)/src/common/ob_rpc_stub.cpp

ob_export_SOURCES = tokenizer.cpp oceanbase_db.cpp db_record_set.cpp \
                    db_utils.cpp db_table_info.cpp db_record.cpp \
                    $(top_builddir)/src/updateserver/ob_ups_stat.cpp ob_export_main.cpp \
                    ob_export_param.cpp ob_export_extra_param.cpp \
                    file_appender.cpp ob_export_producer.cpp \
                    ob_export_consumer.cpp db_tablet_info.cpp \
                    file_writer.cpp db_row_formator.cpp \
                    ob_export_monitor.cpp charset.cpp \
					$(top_builddir)/src/common/ob_general_rpc_stub.cpp $(top_builddir)/src/common/ob_rpc_stub.cpp
obbinlog_SOURCES = obbinlog.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
genfile_SOURCES = gen_file.cpp

ob_import_v2_SOURCES = tokenizer_v2.cpp ob_import_v2.cpp ob_import_producer_v2.cpp \
                    db_utils.cpp file_reader_v2.cpp oceanbase_db.cpp db_record_set.cpp \
                    ob_import_comsumer_v2.cpp ob_import_param_v2.cpp ob_import_main_v2.cpp \
                    db_table_info.cpp db_record.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp \
                    file_appender.cpp db_queue_v2.cpp ob_mysql_client_helper.cpp charset.cpp \
					$(top_builddir)/src/common/ob_general_rpc_stub.cpp $(top_builddir)/src/common/ob_rpc_stub.cpp

do_rs_merge_SOURCES = merge_main.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp 

#bin_PROGRAMS = obdump ob_import tablet_stats obbinlog ob_export
bin_PROGRAMS = ob_import ob_export ob_check ob_import_v2
#bin_PROGRAMS = do_rs_merge
#SUBDIRS=scripts
#DIST_SUBDIRS=scripts
clean-local:
	-rm -f *.gcov *.gcno *.gcda
