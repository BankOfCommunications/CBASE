alter system set enable_load_data='true' server_type=rootserver;
alter system set migrate_band_limit_per_second='30M' server_type=chunkserver;
alter system set balance_max_concurrent_migrate_num ='15' server_type=rootserver;
alter system set balance_max_migrate_out_per_cs ='3' server_type= rootserver;
alter system set balance_max_migrate_in_per_cs ='3' server_type=rootserver;
alter system set max_migrate_task_count ='3' server_type=chunkserver;
alter system set datasource_timeout ='30' server_type=chunkserver;
alter system set load_data_max_timeout_per_range ='5m' server_type=rootserver;
alter system set merge_thread_per_disk ='1' server_type=chunkserver;
alter system set ups_waiting_register_time ='20' server_type=rootserver;

create table load_data_history 
(
  start_time timestamp,
  end_time timestamp,
  table_name varchar(256),
  table_id varchar(32),
  status varchar(256),
  uri varchar(1024),
  primary key(start_time, table_name, table_id)
);

create table collect_info 
(
  rowkey varchar(1024) primary key, 
  user_nick varchar(1024),
  is_shared int,
  note varchar(1024),
  collect_time datetime,
  status int,
  gm_create datetime,
  gm_modified datetime,
  tag varchar(1024),
  category int,
  title varchar(1024),
  picurl varchar(1024),
  owner_id int,
  owner_nick varchar(1024),
  price int,
  ends datetime,
  proper_xml varchar(1024),
  comment_count int,
  collector_count int,
  collect_count int,
  total_money int,
  collect_time_precise datetime
);

create table item_info 
(
  rowkey varchar(1024) primary key, 
  title varchar(1024), 
  picurl varchar(1024), 
  owner_id int, 
  owner_nick varchar(1024),
  price int,
  ends datetime,
  proper_xml varchar(1024),
  comment_count int,
  collector_count int,
  collect_count int,
  money int,
  gm_create datetime,
  gm_modified datetime
);
