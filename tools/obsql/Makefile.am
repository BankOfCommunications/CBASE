AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src 

LDADD = $(top_builddir)/src/chunkserver/libchunkserver.a \
        $(top_builddir)/src/mergeserver/libmergeserver.a \
        $(top_builddir)/src/rootserver/librootserver.a \
        $(top_builddir)/src/updateserver/libupdateserver.a \
	$(top_builddir)/src/common/compress/libcomp.a \
        $(top_builddir)/src/common/libcommon.a \
        $(top_builddir)/src/common/btree/libbtree.a \
        ${TBLIB_ROOT}/lib/libtbnet.a \
        ${TBLIB_ROOT}/lib/libtbsys.a 

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lboost_regex ${GCOV_LIB}
CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall -DORDER_SERIALIZE -DCOMPATIBLE -D_BTREE_ENGINE_ #-O2 -fno-strict-aliasing

bin_PROGRAMS = obsql
obsql_SOURCES = obsql.cpp \
                client_rpc.cpp \
                base_client.cpp \
                ob_server_stats.cpp \
                stats.cpp \
                schema_printer.cpp \
                selectstmt.cpp \
                insertstmt.cpp \
                deletestmt.cpp \
                updatestmt.cpp \
		stmt.cpp \
                common_func.cpp


