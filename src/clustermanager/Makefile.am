AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbnet                \
                          -I${TBLIB_ROOT}/include/tbsys    \
                          -I${top_srcdir}/include          \
                          -I${top_srcdir}/src              \
                          -I${top_srcdir}/src/common       \
                          -I${top_srcdir}/src/updateserver

LDADD = $(top_builddir)/src/sstable/libsstable.a  \
        ${top_builddir}/src/common/libcommon.a    \
        ${TBLIB_ROOT}/lib/libtbnet.a              \
        ${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lc -lm -lrt -lboost_regex
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif


pub_source =                                                            \
  ob_ocm_app_info.h            ob_ocm_app_info.cpp                      \
  ob_ocm_broadcast_task.h      ob_ocm_broadcast_task.cpp                \
  ob_ocm_instance.cpp                                                   \
  ob_ocm_instance.h                                                     \
  ob_ocm_main.h                                                         \
  ob_ocm_meta.h                ob_ocm_meta.cpp                          \
  ob_ocm_meta_manager.h        ob_ocm_meta_manager.cpp                  \
  ob_ocm_param.h               ob_ocm_param.cpp                         \
  ob_ocm_rpc_stub.h            ob_ocm_rpc_stub.cpp                      \
  ob_ocm_server.h              ob_ocm_server.cpp                        \
  ob_ocm_service.h             ob_ocm_service.cpp

noinst_LIBRARIES = libocm.a
bin_PROGRAMS = ocm ocm_admin
libocm_a_SOURCES = ${pub_source}
ocm_SOURCES = ob_ocm_main.cpp ${pub_source}
ocm_admin_SOURCES = ocm_admin.cpp

EXTRA_DIST = ocm.conf.template

pkgdata_data_DATA=ocm.conf.template
pkgdata_datadir=${prefix}/etc

clean-local:
	-rm -f *.gcov *.gcno *.gcda
