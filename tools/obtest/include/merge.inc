alter system set merge_delay_interval='1s' server_type=chunkserver;
alter system set merge_delay_for_lsync='1s' server_type=chunkserver;
alter system set min_drop_cache_wait_time='1s' server_type=chunkserver;
alter system set min_major_freeze_interval='1s' server_type=updateserver;
alter system set merge_thread_per_disk=8  server_type=chunkserver;
