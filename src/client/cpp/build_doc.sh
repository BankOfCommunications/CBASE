#!/bin/sh
doxygen doxygen.conf
sed -i -e "s/charset=iso-8859-1/charset=utf-8/g" doc/html/*html
cp set_sync_demo.c libobapi-1.1.0.tar.gz ../../../oceanbase-dev-1.1.0-0.x86_64.rpm doc/html/
