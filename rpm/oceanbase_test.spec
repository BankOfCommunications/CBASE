#
# (C) 2007-2010 TaoBao Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# oceanbase_test.spec for hudson build test
# http://10.232.4.35:8765/hudson/job/oceanbase_zhuweng/
# Version: $id$
#
# Authors:
#   MaoQi maoqi@alipay.com
#

Name: %NAME
Version: %VERSION
Release: %{RELEASE}
Summary: TaoBao distributed database
Group: Applications/Databases
URL: http://oceanbase.alibaba-inc.com/
Packager: taobao
License: GPL
Vendor: TaoBao
Prefix:%{_prefix}
Source:%{NAME}-%{VERSION}.tar.gz
BuildRoot: %(pwd)/%{name}-root
Requires: lzo = 20:2.0.6 snappy = 20:1.1.2 libaio >= 0.3 openssl >= 0.9.8 perl-DBI
AutoReqProv: no

%package -n oceanbase-utils
summary: OceanBase utility programs
group: Development/Tools
Version: %VERSION
Release: %{RELEASE}

%package -n oceanbase-devel
summary: OceanBase client library
group: Development/Libraries
Version: %VERSION
BuildRequires: t-db-congo-drcmessage >= 0.1.2-40 tb-lua-dev >= 5.1.4
Requires: clrl >= 7.15.5 mysql-devel >= 5.1.0
Release: %{RELEASE}

%description
OceanBase is a distributed database

%description -n oceanbase-utils
OceanBase utility programs

%description -n oceanbase-devel
OceanBase client library

%define _unpackaged_files_terminate_build 0

%prep
%setup

%build
chmod u+x build.sh
./build.sh init
# use the following line for local bulid
./configure RELEASEID=%{RELEASE} --prefix=%{_prefix} --with-test-case=no --with-release=yes CPPFLAGS="-I/Foundation_Platform_20141122" LDFLAGS="-L/home/xiaojun.chengxj/usr_oceanbase/lib -L/home/xiaojun.chengxj/usr_oceanbase/lib64" --with-svnfile
make %{?_smp_mflags}

%install
make DESTDIR=$RPM_BUILD_ROOT install
#pushd $RPM_BUILD_DIR/%{NAME}-%{VERSION}/src/mrsstable
#mvn assembly:assembly
#cp target/mrsstable-1.0.1-SNAPSHOT-jar-with-dependencies.jar $RPM_BUILD_ROOT/%{_prefix}/bin/mrsstable.jar
#popd

mkdir -p $RPM_BUILD_ROOT%{_prefix}/mrsstable_lib_6u
cp $RPM_BUILD_ROOT%{_prefix}/lib/liblzo_1.0.so     $RPM_BUILD_ROOT%{_prefix}/mrsstable_lib_6u/
cp $RPM_BUILD_ROOT%{_prefix}/lib/libmrsstable.so   $RPM_BUILD_ROOT%{_prefix}/mrsstable_lib_6u/
cp $RPM_BUILD_ROOT%{_prefix}/lib/libnone.so        $RPM_BUILD_ROOT%{_prefix}/mrsstable_lib_6u/
cp $RPM_BUILD_ROOT%{_prefix}/lib/libsnappy_1.0.so  $RPM_BUILD_ROOT%{_prefix}/mrsstable_lib_6u/

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(0755, admin, admin)
%dir %{_prefix}/etc
%dir %{_prefix}/bin
%dir %{_prefix}/lib
%config(noreplace) %{_prefix}/etc/schema.ini
%config %{_prefix}/etc/lsyncserver.conf.template
%config %{_prefix}/etc/sysctl.conf
%config %{_prefix}/etc/snmpd.conf
%config %{_prefix}/etc/importserver.conf.template
%config %{_prefix}/etc/importcli.conf.template
%config %{_prefix}/etc/configuration.xml.template
%config %{_prefix}/etc/proxyserver.conf.template
%{_prefix}/bin/rootserver
%{_prefix}/bin/updateserver
%{_prefix}/bin/mergeserver
%{_prefix}/bin/chunkserver
%{_prefix}/bin/proxyserver
%{_prefix}/bin/rs_admin
%{_prefix}/bin/ups_admin
%{_prefix}/bin/log_tool
%{_prefix}/bin/cs_admin
%{_prefix}/bin/ob_ping
%{_prefix}/bin/str2checkpoint
%{_prefix}/bin/checkpoint2str
%{_prefix}/bin/lsyncserver
%{_prefix}/bin/dumpsst
%{_prefix}/bin/importserver.py
%{_prefix}/bin/importcli.py
%{_prefix}/bin/ob_import
%{_prefix}/bin/ob_import_v2
%{_prefix}/bin/ob_export
%{_prefix}/bin/ob_check
#%{_prefix}/bin/mrsstable.jar
%{_prefix}/lib/liblzo_1.0.a
%{_prefix}/lib/liblzo_1.0.la
%{_prefix}/lib/liblzo_1.0.so
%{_prefix}/lib/liblzo_1.0.so.0
%{_prefix}/lib/liblzo_1.0.so.0.0.0
%{_prefix}/lib/libmrsstable.a
%{_prefix}/lib/libmrsstable.la
%{_prefix}/lib/libmrsstable.so
%{_prefix}/lib/libmrsstable.so.0
%{_prefix}/lib/libmrsstable.so.0.0.0
%{_prefix}/lib/libnone.a
%{_prefix}/lib/libnone.la
%{_prefix}/lib/libnone.so
%{_prefix}/lib/libnone.so.0
%{_prefix}/lib/libnone.so.0.0.0
%{_prefix}/lib/libsnappy_1.0.a
%{_prefix}/lib/libsnappy_1.0.la
%{_prefix}/lib/libsnappy_1.0.so
%{_prefix}/lib/libsnappy_1.0.so.0
%{_prefix}/lib/libsnappy_1.0.so.0.0.0
%{_prefix}/mrsstable_lib_5u/liblzo_1.0.so
%{_prefix}/mrsstable_lib_5u/libmrsstable.so
%{_prefix}/mrsstable_lib_5u/libnone.so
%{_prefix}/mrsstable_lib_5u/libsnappy_1.0.so
%{_prefix}/mrsstable_lib_5u/liblzo2.so
%{_prefix}/mrsstable_lib_5u/libsnappy.so
%{_prefix}/mrsstable_lib_6u/liblzo_1.0.so
%{_prefix}/mrsstable_lib_6u/libmrsstable.so
%{_prefix}/mrsstable_lib_6u/libnone.so
%{_prefix}/mrsstable_lib_6u/libsnappy_1.0.so
%{_prefix}/mrsstable_lib_6u/liblzo2.so
%{_prefix}/mrsstable_lib_6u/libsnappy.so
%{_prefix}/bin/oceanbase.pl
%config %{_prefix}/etc/oceanbase.conf.template
%{_prefix}/tests/

%files -n oceanbase-utils
%defattr(0755, admin, admin)
%{_prefix}/bin/ob_import
%{_prefix}/bin/ob_import_v2

%files -n oceanbase-devel
%defattr(0755, admin, admin)
%{_prefix}/lib/liboblog.so.1.0.0
%{_prefix}/lib/liboblog.a
%config(noreplace) %{_prefix}/etc/liboblog.conf
%config(noreplace) %{_prefix}/etc/liboblog.partition.lua
%dir %{_prefix}/include
%{_prefix}/include/ob_define.h
%{_prefix}/include/liboblog.h

%post
chown -R admin:admin $RPM_INSTALL_PREFIX
