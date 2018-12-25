#!/bin/sh

#tests of sstable writer and reader
./test_sstable_schema
./test_sstable_row
./test_sstable_block_builder
./test_sstable_block_index_builder
./test_sstable_writer
./test_sstable_trailer
./test_sstable_reader
./test_sstable_schema_cache

#tests of block cache
./test_blockcache
./test_blockreader
./test_aio_buffer_mgr

#tests of sstable get and scan interface
./test_sstable_getter
./test_sstable_block_reader
./test_sstable_block_scanner
./test_column_group_scanner
./test_sstable_scanner
