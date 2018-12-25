# Copyright (C) 2004 MySQL AB
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
SUBDIRS = drivers tests .

EXTRA_DIST = CMakeLists.txt sb_win.h sb_win.c

if USE_OLTP
bin_PROGRAMS = sysbench
else
bin_PROGRAMS = trxtest
endif

EXTRA_LDFLAGS = @EXTRA_LDFLAGS@
simon_ldflags = -lsimon -lBasicsSubset -lsimon_sysbench -Lxmlconfig -Lclientcommon 

# The following check will be extended as new database drivers will be added
if USE_MYSQL
mysql_ldadd = drivers/mysql/libsbmysql.a $(MYSQL_LIBS)
endif

if USE_ORACLE
ora_ldadd = drivers/oracle/libsboracle.a $(ORA_LIBS)
endif

if USE_PGSQL
pgsql_ldadd = drivers/pgsql/libsbpgsql.a $(PGSQL_LIBS)
endif

if USE_OLTP
sysbench_CPPFLAGS = -Iclientcommon -Ixmlconfig
sysbench_LDFLAGS = $(simon_LDFLAGS) $(EXTRA_LDFLAGS) $(mysql_ldflags) $(pgsql_ldflags) $(ora_ldflags)
sysbench_LDADD = tests/oltp/libsboltp.a  clientcommon/libsimon.a clientcommon/libBasicsSubset.a xmlconfig/libsimon_sysbench.a \
    $(mysql_ldadd) $(pgsql_ldadd) $(ora_ldadd)
sysbench_SOURCES = sysbench.c sysbench.h sb_timer.c sb_timer.h \
sb_options.c sb_options.h sb_logger.c sb_logger.h sb_list.h db_driver.h \
db_driver.c
else
trxtest_SOURCES = sysbench.c sysbench.h sb_timer.c sb_timer.h \
sb_options.c sb_options.h sb_logger.c sb_logger.h sb_list.h db_driver.h \
db_driver.c
trxtest_CPPFLAGS = -Iclientcommon -Ixmlconfig
trxtest_LDFLAGS = $(simon_LDFLAGS) $(EXTRA_LDFLAGS) $(mysql_ldflags) $(pgsql_ldflags) $(ora_ldflags)
trxtest_LDADD = tests/trx/libsbtrx.a  clientcommon/libsimon.a clientcommon/libBasicsSubset.a trxtestxml/libsimon_trxtest.a \
    $(mysql_ldadd) $(pgsql_ldadd) $(ora_ldadd)
endif
