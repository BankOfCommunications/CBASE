AM_CPPFLAGS= -I${TBLIB_ROOT}/include/tbsys \
			 -I${EASY_ROOT}/include/easy \
			 -I${top_srcdir}/include \
			 -I${top_srcdir}/src

AM_LDFLAGS=-lpthread -lc -lm  -lrt -lcrypt -lssl -lboost_regex
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

LDADD = ${EASY_LIB_PATH}/libeasy.a ${TBLIB_ROOT}/lib/libtbsys.a

noinst_LIBRARIES = libroottable.a

libroottable_a_SOURCES = ob_tablet_meta_table.h\
						ob_old_root_table_schema.h ob_old_root_table_schema.cpp\
						ob_tablet_meta_table_row.h ob_tablet_meta_table_row.cpp\
						ob_first_tablet_entry_meta.h ob_first_tablet_entry_meta.cpp\
						ob_first_tablet_entry_schema.h ob_first_tablet_entry_schema.cpp\
						ob_first_tablet_entry.h ob_first_tablet_entry.cpp\
						ob_ms_provider.h\
						ob_scan_helper.h\
						ob_scan_helper_impl.h ob_scan_helper_impl.cpp\
            ob_mutate_helper.h\
            ob_ups_provider.h\
						ob_meta_table_schema.h ob_meta_table_schema.cpp\
						ob_meta_table3.h ob_meta_table3.cpp\
						ob_root_table3.h ob_root_table3.cpp\
						ob_root_table_service.h ob_root_table_service.cpp
clean-local:
	-rm -f *.gcov *.gcno *.gcda
