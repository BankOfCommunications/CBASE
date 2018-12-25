AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src\
			  -I${top_srcdir}/src/common\
			  -I${top_srcdir}/src/updateserver\
			  -I${top_srcdir}/src/clustermanager

CXXFLAGS += -D__UNIT_TEST__
LDADD = \
    $(top_builddir)/src/clustermanager/librootserver.a \
    $(top_builddir)/src/updateserver/libupdateserver.a \
    ${top_builddir}/src/clustermanager/librootserver.a \
    ${top_builddir}/src/common/compress/libcomp.a \
    ${top_builddir}/src/common/libcommon.a \
		${top_builddir}/src/common/btree/libbtree.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a 

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lboost_regex
CXXFLAGS+=-D__VERSION_ID__="\"updateserver 1.0.0.0\""
CXXFLAGS+=-D_BTREE_ENGINE_
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif
noinst_LIBRARIES = libmms.a
libmms_a_SOURCES = \
  ob_monitor.cpp \
	ob_node.cpp\
  ob_mms_checklease_task.cpp\
  ob_mms_heartbeat_task.cpp

EXTRA_DIST = \
  ob_monitor.h \
	ob_node.h\
  ob_mms_checklease_task.h\
  ob_mms_heartbeat_task.h
