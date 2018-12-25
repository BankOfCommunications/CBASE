
/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *   Version: 0.1
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - base data structure, maybe modify in future
 *
 */
#ifndef OCEANBASE_COMMON_OB_TABLET_INFO_H_
#define OCEANBASE_COMMON_OB_TABLET_INFO_H_

#include <tblog.h>
#include "ob_define.h"
#include "ob_server.h"
#include "ob_array_helper.h"
#include "page_arena.h"
#include "ob_range2.h"
//add wenghaixing [secondary index col checksum.h]20141210
#include "common/ob_column_checksum.h"
//add e
//add liumz, [secondary index static_index_build] 20150320:b
#include "common/ob_tablet_histogram.h"
//add:e
namespace oceanbase
{
  namespace common
  {
    struct ObTabletLocation
    {
      int64_t tablet_version_;
      int64_t tablet_seq_;
      ObServer chunkserver_;

      ObTabletLocation() :tablet_version_(0), tablet_seq_(0) { }
      ObTabletLocation(int64_t tablet_version, const ObServer & server)
        :tablet_version_(tablet_version), tablet_seq_(0),
         chunkserver_(server) { }

      ObTabletLocation(int64_t tablet_version, int64_t tablet_seq,
        const ObServer & server)
        :tablet_version_(tablet_version), tablet_seq_(tablet_seq),
         chunkserver_(server) { }

      static void dump(const ObTabletLocation & location);

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObTabletInfo
    {
      static const int64_t DEFAULT_VERSION = 1;
      static const int64_t RESERVED_LEN = 1;
      ObNewRange range_;
      int64_t row_count_;
      int64_t occupy_size_;
      uint64_t crc_sum_;
      uint64_t row_checksum_;
      int16_t version_;
      int16_t reserved16_;
      int32_t reserved32_;
      int64_t reserved64_;

      ObTabletInfo()
        : range_(), row_count_(0), occupy_size_(0), crc_sum_(0) , row_checksum_(0), version_(DEFAULT_VERSION){}
      ObTabletInfo(const ObNewRange& r, const int32_t rc, const int32_t sz, const uint64_t crc_sum = 0, const uint64_t row_checksum = 0)
        : range_(r), row_count_(rc), occupy_size_(sz), crc_sum_(crc_sum), row_checksum_(row_checksum), version_(DEFAULT_VERSION){}

      inline bool equal(const ObTabletInfo& tablet) const
      {
        return range_.equal(tablet.range_);
      }
      template <typename Allocator>
      int deep_copy(Allocator &allocator, const ObTabletInfo &other);
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    template <typename Allocator>
    inline int ObTabletInfo::deep_copy(Allocator &allocator, const ObTabletInfo &other)
    {
      int ret = OB_SUCCESS;
      this->row_count_ = other.row_count_;
      this->occupy_size_ = other.occupy_size_;
      this->crc_sum_ = other.crc_sum_;
      this->row_checksum_ = other.row_checksum_;

      ret = deep_copy_range(allocator, other.range_, this->range_);
      return ret;
    }

    struct ObTabletReportInfo
    {
      ObTabletInfo tablet_info_;
      ObTabletLocation tablet_location_;

      bool operator== (const ObTabletReportInfo &other) const;
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObTabletReportInfoList
    {
      ObTabletReportInfo tablets_[OB_MAX_TABLET_LIST_NUMBER];
      ObArrayHelper<ObTabletReportInfo> tablet_list_;
      CharArena allocator_;

      ObTabletReportInfoList()
      {
        reset();
      }

      void reset()
      {
        tablet_list_.init(OB_MAX_TABLET_LIST_NUMBER, tablets_);
      }

      inline int add_tablet(const ObTabletReportInfo& tablet)
      {
        int ret = OB_SUCCESS;

        if (!tablet_list_.push_back(tablet))
          ret = OB_ARRAY_OUT_OF_RANGE;

        return ret;
      }

      inline int rollback(ObTabletReportInfoList& rollback_list, const int64_t rollback_count)
      {
        int ret = OB_SUCCESS;
        int64_t tablet_size = get_tablet_size();

        if (rollback_count > 0 && rollback_count <= tablet_size && rollback_count <= OB_MAX_TABLET_LIST_NUMBER)
        {
          for (int64_t i = 0; i < rollback_count && OB_SUCCESS == ret; i++)
          {
            ret = rollback_list.add_tablet(tablets_[tablet_size - rollback_count + i]);
          }
          if (OB_SUCCESS == ret)
          {
            for (int64_t i = 0; i < rollback_count; i++)
            {
              tablet_list_.pop();
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN, "invalid rollback count, rollback_count=%ld, tablet_size=%ld",
              rollback_count, tablet_size);
          ret = OB_ERROR;
        }

        return ret;
      }

      inline int64_t get_tablet_size() const { return tablet_list_.get_array_index(); }
      inline const ObTabletReportInfo* const get_tablet() const { return tablets_; }
      bool operator== (const ObTabletReportInfoList &other) const;
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObIndexTabletReportInfoList
    {
      ObTabletReportInfo tablets_[OB_MAX_TABLET_NUMBER_PER_TABLE];
      ObArrayHelper<ObTabletReportInfo> tablet_list_;
      CharArena allocator_;

      ObIndexTabletReportInfoList()
      {
        reset();
      }

      void reset()
      {
        tablet_list_.init(OB_MAX_TABLET_NUMBER_PER_TABLE, tablets_);
      }

      inline int add_tablet(const ObTabletReportInfo& tablet)
      {
        int ret = OB_SUCCESS;

        if (!tablet_list_.push_back(tablet))
          ret = OB_ARRAY_OUT_OF_RANGE;

        return ret;
      }

      inline int rollback(ObIndexTabletReportInfoList& rollback_list, const int64_t rollback_count)
      {
        int ret = OB_SUCCESS;
        int64_t tablet_size = get_tablet_size();

        if (rollback_count > 0 && rollback_count <= tablet_size && rollback_count <= OB_MAX_TABLET_NUMBER_PER_TABLE)
        {
          for (int64_t i = 0; i < rollback_count && OB_SUCCESS == ret; i++)
          {
            ret = rollback_list.add_tablet(tablets_[tablet_size - rollback_count + i]);
          }
          if (OB_SUCCESS == ret)
          {
            for (int64_t i = 0; i < rollback_count; i++)
            {
              tablet_list_.pop();
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN, "invalid rollback count, rollback_count=%ld, tablet_size=%ld",
              rollback_count, tablet_size);
          ret = OB_ERROR;
        }

        return ret;
      }

      inline int64_t get_tablet_size() const { return tablet_list_.get_array_index(); }
      inline const ObTabletReportInfo* const get_tablet() const { return tablets_; }
      bool operator== (const ObIndexTabletReportInfoList &other) const;
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObIndexTabletInfoList
    {
      ObTabletInfo tablets[OB_MAX_TABLET_NUMBER_PER_TABLE];
      ObArrayHelper<ObTabletInfo> tablet_list;

      ObIndexTabletInfoList()
      {
        reset();
      }

      void reset()
      {
        tablet_list.init(OB_MAX_TABLET_NUMBER_PER_TABLE, tablets);
      }

      inline int add_tablet(const ObTabletInfo& tablet)
      {
        int ret = OB_SUCCESS;

        if (!tablet_list.push_back(tablet))
          ret = OB_ARRAY_OUT_OF_RANGE;

        return ret;
      }

      inline int64_t get_tablet_size() const { return tablet_list.get_array_index(); }
      inline const ObTabletInfo* const get_tablet() const { return tablets; }

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObTabletInfoList
    {
      ObTabletInfo tablets[OB_MAX_TABLET_LIST_NUMBER];
      ObArrayHelper<ObTabletInfo> tablet_list;

      ObTabletInfoList()
      {
        reset();
      }

      void reset()
      {
        tablet_list.init(OB_MAX_TABLET_LIST_NUMBER, tablets);
      }

      inline int add_tablet(const ObTabletInfo& tablet)
      {
        int ret = OB_SUCCESS;

        if (!tablet_list.push_back(tablet))
          ret = OB_ARRAY_OUT_OF_RANGE;

        return ret;
      }

      inline int64_t get_tablet_size() const { return tablet_list.get_array_index(); }
      inline const ObTabletInfo* const get_tablet() const { return tablets; }

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObTableImportInfoList
    {
      uint64_t tables_[OB_MAX_TABLE_NUMBER];
      ObArrayHelper<uint64_t> table_list_;
      int64_t tablet_version_;
      bool import_all_;
      bool response_rootserver_;
      bool is_sorted_;

      ObTableImportInfoList()
      {
        reset();
      }

      void reset()
      {
        table_list_.init(OB_MAX_TABLE_NUMBER, tables_);
        tablet_version_ = 0;
        import_all_ = true;
        response_rootserver_ = true;
        is_sorted_ = false;
      }

      inline int add_table(const uint64_t& table_id)
      {
        int ret = OB_SUCCESS;

        if (!table_list_.push_back(table_id))
          ret = OB_ARRAY_OUT_OF_RANGE;

        return ret;
      }

      inline void sort()
      {
        if (!is_sorted_ && table_list_.get_array_index() > 0)
        {
          std::sort(tables_, tables_ + table_list_.get_array_index());
          is_sorted_ = true;
        }
      }

      inline bool is_table_exist(const uint64_t table_id) const
      {
        bool bret = false;
        int64_t size = table_list_.get_array_index();

        if (import_all_)
        {
          bret = true;
        }
        else if (is_sorted_ && size > 0)
        {
          bret = std::binary_search(tables_, tables_ + size, table_id);
        }

        return bret;
      }

      inline int64_t get_table_size() const { return table_list_.get_array_index(); }
      inline const uint64_t* const get_table() const { return tables_; }
      int64_t to_string(char* buffer, const int64_t length) const;

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

  } // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_ROOTSERVER_COMMONDATA_H_

