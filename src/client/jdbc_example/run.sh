#!/bin/bash
#
# AUTHOR: Zhifeng YANG
# DATE: 2013-01-13
# DESCRIPTION:
#

mvn package -Dmaven.test.skip=true
mvn assembly:single
java -jar target/objdbcexample-0.0.1-SNAPSHOT-jar-with-dependencies.jar
