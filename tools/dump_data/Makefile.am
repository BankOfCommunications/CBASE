AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I$(top_builddir)/src/common/ \
        -I${top_srcdir}/src \
	      -I${EASY_ROOT}/include/easy   \
        -I${JAVA_HOME}/include \
	      -I${JAVA_HOME}/include/linux

LDADD = $(top_builddir)/src/common/libcommon.a \
        ${EASY_LIB_PATH}/libeasy.a \
		    ${TBLIB_ROOT}/lib/libtbsys.a 

AM_LDFLAGS = -lpthread -lc -lm -lrt -lssl -ldl -lhdfs -ljvm -lboost_regex -L${JAVA_HOME}/jre/lib/amd64/server/ -L${HADOOP_HOME}/libhdfs/
CXXFLAGS = -g -Wall -DORDER_SERIALIZE

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libdumpdata.a
libdumpdata_a_SOURCES = task_info.cpp \
                        task_manager.cpp \
                        task_output.cpp \
                        task_factory.cpp \
                        tablet_location.cpp \
                        rpc_stub.cpp \
                        task_server.cpp \
                        task_worker.cpp \
                        base_server.cpp \
                        base_client.cpp \
												task_stats.cpp

bin_PROGRAMS = task_server task_worker dump2str parse_file

test_hdfs_SOURCES = test_hdfs.cpp
dump2str_SOURCES = dump2str.cpp
parse_file_SOURCES = parse_file.cpp

task_server_SOURCES = task_server_main.cpp \
                     task_server_param.cpp \
                     task_info.cpp \
                     task_output.cpp \
                     task_manager.cpp \
                     task_factory.cpp \
                     rpc_stub.cpp \
                     tablet_location.cpp \
                     task_server.cpp \
                     base_server.cpp \
										 task_utils.cpp \
										 task_table_conf.cpp \
                     task_server_callback.cpp

task_worker_SOURCES = task_worker_main.cpp \
                     task_info.cpp \
                     rpc_stub.cpp \
                     tablet_location.cpp \
                     task_worker.cpp \
                     base_client.cpp \
										 task_output_file.cpp \
										 task_worker_param.cpp \
										 posix_env.cpp \
										 hdfs_env.cpp \
										 task_utils.cpp \
										 task_table_conf.cpp \
										 task_stats.cpp \
										 hdfs_ext.cpp

EXTRA_DIST = base_client.h \
	     base_server.h \
	     hdfs_env.h \
	     hdfs_ext.h \
	     hdfs.h \
	     parse_file.h \
	     posix_env.h \
	     rpc_stub.h \
	     tablet_location.h \
	     task_env.h \
	     task_factory.h \
	     task_info.h \
	     task_manager.h \
	     task_output_file.h \
	     task_output.h \
	     task_packet.h \
	     task_server.h \
	     task_server_param.h \
	     task_stats.h \
	     task_table_conf.h \
       task_server_callback.h \
	     task_utils.h \
	     task_worker.h \
	     task_worker_param.h \
       hdfsJniHelper.h \
       ob_task_server.sh \
       ob_task_slave.sh \
       merge_tmp.py \
       obcleanup.py

pkgdata_data_DATA=ob_task_server.sh ob_task_slave.sh merge_tmp.py obcleanup.py
pkgdata_datadir=${prefix}/etc

clean-local:
	-rm -f *.gcov *.gcno *.gcda
