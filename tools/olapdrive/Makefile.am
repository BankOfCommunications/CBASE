AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/tools/syschecker \
			  -I${top_srcdir}/src

LDADD = $(top_builddir)/src/chunkserver/libchunkserver.a \
		$(top_builddir)/tools/syschecker/client/libclient.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/libcommon.a \
		${EASY_LIB_PATH}/libeasy.a \
		${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -lboost_regex ${GCOV_LIB}
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif


bin_PROGRAMS = olapdrive

olapdrive_SOURCES = main.cpp \
					ob_olapdrive_main.cpp \
					ob_olapdrive.cpp \
					ob_olapdrive_param.cpp \
					ob_write_worker.cpp \
					ob_read_worker.cpp \
					ob_olapdrive_stat.cpp \
					ob_olapdrive_meta.cpp \
					ob_olapdrive_schema.cpp \
					ob_campaign_effect.cpp \
					ob_campaign_impression.cpp \
					ob_platform_pvtype_effect.cpp \
					ob_platform_pvtype_effect_daily.cpp \
					ob_adgroup_effect_top10.cpp \
					ob_adgroup_impression_count.cpp \
					ob_adgroup_effect_daily.cpp \
					ob_adgroup_bidword_effect_orderby.cpp \
					ob_bidword_effect_top10.cpp \
					ob_bidword_effect_daily.cpp \
					ob_several_adgroup_effect.cpp \
					ob_several_bidword_effect.cpp \
					ob_several_creative_effect.cpp \
					ob_lz_query.cpp

EXTRA_DIST = ob_olapdrive_main.h \
			 ob_olapdrive.h \
			 ob_olapdrive_param.h \
			 ob_write_worker.h \
			 ob_read_worker.h \
			 ob_olapdrive_stat.h \
			 ob_olapdrive_schema.h \
			 ob_lz_common.h \
			 ob_lz_query.h \
			 ob_campaign_effect.h \
			 ob_campaign_impression.h \
			 ob_platform_pvtype_effect.h \
			 ob_platform_pvtype_effect_daily.h \
			 ob_adgroup_effect_top10.h \
			 ob_adgroup_impression_count.h \
			 ob_adgroup_effect_daily.h \
			 ob_adgroup_bidword_effect_orderby.h \
			 ob_bidword_effect_top10.h \
			 ob_bidword_effect_daily.h \
			 ob_several_adgroup_effect.h \
			 ob_several_bidword_effect.h \
			 ob_several_creative_effect.h\
			 ob_olapdrive_meta.h
