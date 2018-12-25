CREATE TABLE sbtest (id int  NOT NULL auto_increment, k int  NOT NULL default '0', c char(120) NOT NULL default '', pad char(60) NOT NULL default '', PRIMARY KEY  (id));
alter system set merge_delay_interval='1s' server_type=chunkserver;
alter system set merge_delay_for_lsync='1s' server_type=chunkserver;
alter system set min_drop_cache_wait_time='1s' server_type=chunkserver;
create table obstress(name varchar primary key, max_id varchar);

replace into obstress values('test_1',0);
replace into obstress values('test_2',0);
replace into obstress values('test_3',0);
replace into obstress values('test_4',0);
replace into obstress values('test_5',0);
replace into obstress values('test_6',0);
replace into obstress values('test_7',0);
replace into obstress values('test_8',0);
replace into obstress values('test_9',0);
replace into obstress values('test_10',0);
