AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/clustermanager

LDADD = ${top_builddir}/src/clustermanager/libocm.a \
        ${top_builddir}/src/common/libcommon.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a  

AM_LDFLAGS=-lpthread -lc -lm -lrt -lgtest -lboost_regex ${GCOV_LIB}

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = test_ocm_instance test_ocm_app test_ocm_meta test_ocm_meta_manager 

test_ocm_instance_SOURCES = test_ocm_instance.cpp
test_ocm_app_SOURCES = test_ocm_app.cpp
test_ocm_meta_SOURCES = test_ocm_meta.cpp
test_ocm_meta_manager_SOURCES = test_ocm_meta_manager.cpp

EXTRA_DIST = 

clean-local:
	-rm -f *.gcov *.gcno *.gcda
