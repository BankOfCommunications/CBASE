AM_CPPFLAGS = -I${top_srcdir}/tools/dump_data \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common/btree \
			  -I${TBLIB_ROOT}/include/tbnet \
        -I${TBLIB_ROOT}/include/tbsys

LDADD = $(top_builddir)/src/common/btree/libbtree.a \
		    $(top_builddir)/tools/dump_data/libdumpdata.a \
        $(top_builddir)/src/common/libcommon.a \
				${TBLIB_ROOT}/lib/libtbnet.a \
				${TBLIB_ROOT}/lib/libtbsys.a

CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall 
AM_LDFLAGS = -lpthread -lc -lm -lrt -L. -ljvm -lhdfs -lgtest -lboost_regex

pub_env_source = ../hdfs_env.cpp ../posix_env.cpp ../task_output_file.cpp ../task_utils.cpp ../task_worker_param.cpp ../task_table_conf.cpp 
pub_source = test_mock_root_server.cpp

bin_PROGRAMS = test_tablet_location \
              test_task_info \
              test_task_factory \
              test_task_output \
							test_env \
							test_stats

#bin_PROGRAMS = test_env test_output_file test_worker_param
#bin_PROGRAMS = test_table_conf test_worker_param test_output_file


test_tablet_location_SOURCES = test_tablet_location.cpp
test_task_info_SOURCES = test_task_info.cpp
test_task_output_SOURCES = test_task_output.cpp
test_task_manager_SOURCES = test_task_manager.cpp
test_task_factory_SOURCES = test_task_factory.cpp ${pub_source}
test_env_SOURCES = test_env.cpp ${pub_env_source}
test_output_file_SOURCES = test_output_file.cpp ${pub_env_source}
test_worker_param_SOURCES = test_worker_param.cpp ${pub_env_source}
test_table_conf_SOURCES = test_table_conf.cpp  ${pub_env_source}
test_stats_SOURCES = test_stats.cpp ../task_stats.cpp ${pub_env_source}

EXTRA_DIST = test_mock_root_server.h
