drop database if exists rizhao;
create database rizhao;
use rizhao;
drop table if exists sqltest_table;
create table sqltest_table (
    prefix bigint,
    suffix bigint,
    column1 bigint,
    column2 varchar(20),
    column3 bigint,
    column4 varchar(20),
    timestamp bigint,
    primary key(prefix, suffix));
