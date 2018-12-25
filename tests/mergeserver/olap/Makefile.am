AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/common/btree \
			  -I${top_srcdir}/src/sstable \
			  -I${top_srcdir}/src/mergeserver \
			  -I${top_srcdir}/tests/updateserver 

LDADD = $(top_builddir)/src/mergeserver/libmergeserver.a \
				$(top_builddir)/src/sstable/libsstable.a \
				$(top_builddir)/src/common/libcommon.a \
				$(top_builddir)/src/common/btree/libbtree.a \
				$(top_builddir)/src/common/compress/libcomp.a \
				${TBLIB_ROOT}/lib/libtbnet.a \
				${TBLIB_ROOT}/lib/libtbsys.a

CXXFLAGS += -D_MS_MOCK_WHOLE_  -D_BTREE_ENGINE_ -D__UNIT_TEST__
AM_LDFLAGS = -lpthread -lc -lm  -lrt -lboost_regex ${GCOV_LIB} -ldl
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = gen_range \
			   gen_ups_sst \
			   olappress

gen_range_SOURCES = gen_range.cpp
gen_ups_sst_SOURCES = gen_ups_sst.cpp
olappress_SOURCES = olappress.cpp \
					olap_utils.cpp \
					select_where_order_limit.cpp \
					select_where_comp_order_limit.cpp \
					count_all.cpp \
					get_all_column.cpp \
					get_not_exist_rows.cpp\
					multi_parellel_get.cpp \
					case_list.cpp \
					olap_conf.cpp


EXTRA_DIST = base_case.h\
			 case_list.h\
			 count_all.h\
			 get_all_column.h\
			 get_not_exist_rows.h\
			 multi_parellel_get.h\
			 olap_client.h\
			 olap_conf.h\
			 olap.h\
			 select_where_comp_order_limit.h\
			 select_where_order_limit.h
