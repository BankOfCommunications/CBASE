AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/clustermanager \
			  -I${top_srcdir}/src/mms

LDADD = ${top_builddir}/src/mms/libmms.a \
        ${top_builddir}/src/clustermanager/libocm.a \
        ${top_builddir}/src/common/libcommon.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a  

AM_LDFLAGS=-lpthread -lc -lm -lrt -lgtest -lboost_regex  ${GCOV_LIB}
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif
bin_PROGRAMS = test_mms 

test_mms_SOURCES = test_mms.cpp mock_node_server.cpp mock_server.cpp mock_monitor_server.cpp mock_monitor_server.h  mock_node_server.h  mock_server.h

