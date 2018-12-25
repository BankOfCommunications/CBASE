CREATE TABLE sbtest (id int  NOT NULL auto_increment, k int  NOT NULL default '0', c char(120) NOT NULL default '', pad char(60) NOT NULL default '', PRIMARY KEY  (id)) consistent_mode=static;
alter system set merge_delay_interval='1s' server_type=chunkserver;
alter system set merge_delay_for_lsync='1s' server_type=chunkserver;
alter system set min_drop_cache_wait_time='1s' server_type=chunkserver;
