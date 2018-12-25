AM_LDFLAGS = -L/usr/lib64/mysql/ -lmysqlclient -lgtest -lpthread -lboost_regex

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS=newsqltest sqlquery ob_tablet_join_test preparetest pstest sessiontest
newsqltest_SOURCES=ob_new_sql_test.cpp
sqlquery_SOURCES=ob_sql_query.cpp
ob_tablet_join_test_SOURCES=ob_tablet_join_test.cpp
preparetest_SOURCES=ob_prepare_stmt_test.cpp
pstest_SOURCES=ob_ps_test.cpp
sessiontest_SOURCES=ob_session_test.cpp
