AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys					\
							-I${TBLIB_ROOT}/include/tbnet         \
							-I${EASY_ROOT}/include/easy           \
							-I${top_srcdir}/tools   	            \
							-I${top_srcdir}/src   	              \
							-I${top_srcdir}/src/common						\
							-I${DRC_ROOT}/include/drcmessage

AM_LDFLAGS = -L${top_srcdir}/tools/liboblog/.libs -loblog 		\
						 -lgtest -lssl -lboost_regex

LDADD = ${EASY_LIB_PATH}/libeasy.a
CXXFLAGS += -g -D__DENSE_AVAILABLE__

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = test_log_fetcher test_meta_manager test_main_process test_mysql_adaptor sample sample_r

test_log_fetcher_SOURCES = test_log_fetcher.cpp
test_meta_manager_SOURCES = test_meta_manager.cpp
test_main_process_SOURCES = test_main_process.cpp
test_mysql_adaptor_SOURCES = test_mysql_adaptor.cpp
sample_SOURCES = sample.cpp
sample_r_SOURCES = sample_r.cpp

