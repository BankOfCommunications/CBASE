#ifndef OB_INDEX_BUILDER_H
#define OB_INDEX_BUILDER_H

#include "common/ob_define.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_sstable_reader.h"
#include "ob_get_scan_proxy.h"
#include "ob_get_cell_stream_wrapper.h"
#include "ob_cs_query_agent.h"
#include "ob_chunk_index_worker.h"
#include "common/ob_schema_manager.h"
#include "sql/ob_tablet_scan.h"
#include "ob_agent_scan.h"
#include "common/ob_tablet_info.h"
//add zhuyanchao[secondary index static_index_build.report] 20150325
#include "common/ob_tablet_histogram_report_info.h"
#include "common/ob_tablet_histogram.h"
#include "ob_tablet.h"
//add e

namespace oceanbase
{
  namespace commmon
  {
    class ObTableSchema;
    class ObMergerSchemaManager;
    class ObTabletHistogramReportInfo;
    class ObStaticIndexHistogram;
    class ObTabletSample;
  }

  namespace sstable
  {
    class ObSSTableSchema;
    class ObSSTableWriter;
  }

  namespace chunkserver
  {
    class ObTabletManager;
    class ObTablet;
    class ObIndexWorker;
    //typedef hash::ObHashMap<ObNewRange,ObServer,hash::NoPthreadDefendMode> RangeServerHash;
    //typedef hash::ObHashMap<ObNewRange,ObServer,hash::NoPthreadDefendMode>::const_iterator HashIterator;
    ///这个目录仅仅是做测试之用，实际使用要改成可以配置
    const char* const DEFAULT_SORT_DUMP_FILE = "/data/sort.dmp";
    class ObIndexBuilder
    {
      public:
        static const int64_t DEFAULT_ESTIMATE_ROW_COUNT = 256*1024LL;
        static const int64_t DEFAULT_MEMORY_LIMIT = 256*1024*1024LL;
      public:
        ObIndexBuilder(ObIndexWorker* worker, ObMergerSchemaManager* manager, ObTabletManager* tablet_manager);
        ~ObIndexBuilder() {}
        int init();
        int start(ObTablet* tablet, int64_t sample_rate);
        int get_failed_fake_range(ObNewRange &range);
        //add zhuyanchao[secondary index static_index_build.report] 20150320
        void reset_report_info()
        {
            static_index_hisgram.init();
            ObTabletHistogramReportInfo *temp_tablet_histogram_report = get_tablet_histogram_report_info();
            ObTabletHistogram *temp_tablet_histogram = get_tablet_histogram();
            memset(&(temp_tablet_histogram_report->tablet_info_), 0, sizeof(ObTabletInfo));
            temp_tablet_histogram_report->static_index_histogram_.init();
            temp_tablet_histogram_report->tablet_location_.chunkserver_.reset();
            temp_tablet_histogram_report->tablet_location_.tablet_seq_ = 0;
            temp_tablet_histogram_report->tablet_location_.tablet_version_ = 0;
            temp_tablet_histogram->init();
        }
        void reset_sstable_writer()
        {
          sstable_writer_.reset_close_file();
        }

        ObTabletHistogramReportInfo *get_tablet_histogram_report_info()
        {
            return &static_index_report_info;
        }
        ObTabletHistogram *get_tablet_histogram()
        {
            return &static_index_hisgram;
        }
        common::ModuleArena* get_allocator()
        {
            return &allocator_;
        }
        int update_local_index_meta(ObTablet* tablet,const bool sync_meta);//add liumz[sync local index meta]20161028
        //add zhuyanchao[secondary index static_data_build]20150401
        int update_meta_index(ObTablet* tablet,const bool sync_meta);
        //add e
        //add zhuyanchao[secondary index static_data_build]20150401
        int construct_index_tablet_info(ObTablet* tablet);
        //add e
        //add zhuyanchao[secondary index static_data_build]20150403
        int calc_tablet_col_checksum_index(const ObRow& row, ObRowDesc desc, char *column_checksum, int64_t tid);
        //add e
        //add liuxiao [secondary index col checksum]20150529
        int calc_tablet_col_checksum_index_local_tablet(const ObRow& row, ObRowDesc &rowkey_desc, ObRowDesc &index_desc, char *column_checksum, int64_t tid);
        //add e
        int start_total_index(ObNewRange* range);
		        void get_sstable_id(sstable::ObSSTableId & sstable_id);
        inline int32_t get_disk_no()
        {
          return disk_no_;
        }

      private:
        void cal_sample_rate(const int64_t tablet_row_count, int64_t &sample_rate);

        int gen_index_table_list(uint64_t data_table_id, IndexList &list);

        int write_partitional_index_v1(ObNewRange range, uint64_t index_tid, int32_t disk, int64_t tablet_row_count, int64_t sample_rate);
        int create_new_sstable(uint64_t table_id, int32_t disk);

        int fill_sstable_schema(const uint64_t table_id, const common::ObSchemaManagerV2& common_schema,
                                sstable::ObSSTableSchema& sstable_schema);

        int save_current_row();

        int fill_scan_param(ObScanParam &param);

        int write_total_index_v1();

        int cons_row_desc(ObRowDesc &desc);

        int trans_row_to_sstrow(ObRowDesc &desc, const ObRow &row, ObSSTableRow &sst_row);

        //add zhuyanchao[secondary index static_index_build.columnchecksum]20150407
        int cons_row_desc_without_virtual(ObRowDesc &desc);
        int cons_row_desc_local_tablet(ObRowDesc &desc,ObRowDesc &rowkey_desc,ObRowDesc &index_column_desc,
                                       uint64_t tid ,uint64_t index_tid);
        int push_cid_in_desc_and_ophy(uint64_t index_tid, uint64_t data_tid, const ObRowDesc index_data_desc, ObArray<uint64_t> &basic_columns, ObRowDesc &desc);
        int check_tablet(const ObTablet* tablet);
        //add e

      private:
        //add zhuyanchao
        common::col_checksum cc;
        //add liuxiao [secondary index col checksum] 20150525
        common::col_checksum column_checksum_;
        //add e
        ObTabletExtendInfo index_extend_info_;
        common::ModuleArena allocator_;//存放range
        ObTabletHistogramReportInfo static_index_report_info;//add to tablet_report_info_list
        ObTabletHistogram static_index_hisgram;//for construct tablet_report_info,在初始化的时候用下面的内存分配器分配内存
        //ObTabletSample index_sample;//for construct static_index_histogram
        //int sample_count;//用来记录数据和行数使得可以构造sample
        const common::ObTableSchema* new_table_schema_;
        uint64_t table_id_;
        common::ObNewRange new_range_;
        sstable::ObSSTableId sstable_id_;
        char path_[common::OB_MAX_FILE_NAME_LENGTH];
        int32_t disk_no_;
        common::ObVector<ObTablet *> tablet_arrary_;
        sstable::ObSSTableWriter sstable_writer_;
        ObIndexWorker* worker_;
        common::ObMergerSchemaManager* manager_;
        const common::ObSchemaManagerV2* current_schema_manager_;
        ObTabletManager* tablet_manager_;
        ObSSTableSchema sstable_schema_;
        ObSSTableRow row_;
        int64_t frozen_version_;
        int64_t current_sstable_size_;
        ObString path_string_;
        ObString compressor_string_;
        //add wenghaixing [secondary index static_index_build.cs_scan]20150323
        ObGetCellStreamWrapper    ms_wrapper_;//假装这里有一个mergeserver，用来与其他的CS通信
        ObCSQueryAgent              query_agent_;
        RangeServerHash* r_s_hash_;
        ObSort sort_;

        //add e

    };
  }
}


#endif // OB_INDEX_BUILDER_H
