# rules to auto generate svn_version.cpp
BUILT_SOURCES=$(top_srcdir)/svn_version.cpp $(top_srcdir)/svn_dist_version
CLEANFILES=$(top_srcdir)/svn_version.cpp $(top_srcdir)/svn_version.c $(top_srcdir)/svn_dist_version
$(top_srcdir)/svn_version.cpp: FORCE
	echo -n 'const char* svn_version() { const char* SVN_Version = "' > $@
	@mysvnversion@ >> $(top_srcdir)/svn_version.cpp
	echo '"; return SVN_Version; }' >> $@
	echo 'const char* build_date() { return __DATE__; }' >> $@
	echo 'const char* build_time() { return __TIME__; }' >> $@
	echo -n 'const char* build_flags() { return "' >> $@ && echo -n $(CXXFLAGS) $(CPPFLAGS) |sed s/\"//g >> $@ && echo '"; }' >> $@
	cp $@ $(top_srcdir)/svn_version.c

if HAVESVNWC
$(top_srcdir)/svn_dist_version: FORCE
	@mysvnversion@ > $@
endif

FORCE:
