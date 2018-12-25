/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_olapdrive_meta.h for define olapdrive meta table 
 * operation. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_META_H
#define OCEANBASE_OLAPDRIVE_META_H

#include "common/hash/ob_hashmap.h"
#include "client/ob_client.h"
#include "ob_olapdrive_schema.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    static const int64_t KEY_META_ROWKEY_SIZE = 8;
    static const int64_t CAMPAIGN_ROWKEY_SIZE = 16;
    static const int64_t ADGROUP_ROWKEY_SIZE = 24;
    static const int64_t MAX_OLAPDRIVE_ROWKEY_COLUMN_COUNT = 6;
    
    struct ObKeyMeta
    {
      void reset();
      bool is_valid();
      void display() const;

      int64_t max_unit_id_;
      int64_t min_date_;
      int64_t max_date_;
      int64_t max_campaign_id_;
      int64_t max_adgroup_id_;
      int64_t max_key_id_;
      int64_t max_creative_id_;
      int64_t daily_unit_count_;
    };

    struct ObCampaign
    {
      void reset();
      void display() const;

      int64_t campaign_id_;
      int64_t start_adgroup_id_;
      int64_t end_adgroup_id_;
      int64_t start_id_;
      int64_t end_id_;
    };

    struct ObCampaignKey
    {
      int64_t unit_id_;
      int64_t campaign_id_;

      ObCampaignKey()
      {
        unit_id_ = 0;
        campaign_id_ = 0;
      }

      ObCampaignKey(const int64_t unit_id, const int64_t campaign_id)
      {
        unit_id_ = unit_id;
        campaign_id_ = campaign_id;
      }

      int64_t hash() const
      {
        return common::murmurhash2(this, sizeof(ObCampaignKey), 0);
      };

      bool operator == (const ObCampaignKey &other) const
      {
        return (unit_id_ == other.unit_id_
                && campaign_id_ == other.campaign_id_);
      };
    };

    struct ObAdgroup
    {
      void reset();
      void display() const;

      int64_t campaign_id_;
      int64_t adgroup_id_;
      int64_t start_id_;
      int64_t end_id_;
    };

    struct ObAdgroupKey
    {
      int64_t unit_id_;
      int64_t campaign_id_;
      int64_t adgroup_id_;

      ObAdgroupKey()
      {
        unit_id_ = 0;
        campaign_id_ = 0;
        adgroup_id_ = 0;
      }

      ObAdgroupKey(const int64_t unit_id, const int64_t campaign_id,
                   const int64_t adgroup_id)
      {
        unit_id_ = unit_id;
        campaign_id_ = campaign_id;
        adgroup_id_ = adgroup_id;
      }

      int64_t hash() const
      {
        return common::murmurhash2(this, sizeof(ObAdgroupKey), 0);
      };

      bool operator == (const ObAdgroupKey &other) const
      {
        return (unit_id_ == other.unit_id_
                && campaign_id_ == other.campaign_id_
                && adgroup_id_ == other.adgroup_id_);
      };
    };

    class ObOlapdriveMeta
    {
    public:
      ObOlapdriveMeta(ObOlapdriveSchema& olapdrive_schema, 
                      client::ObClient& client);
      ~ObOlapdriveMeta();

      int init();

      void set_rowkey_meta(ObKeyMeta& key_meta);
      const ObKeyMeta& get_rowkey_meta() const;
      ObKeyMeta& get_rowkey_meta();

      int write_rowkey_meta();

      int get_campaign(const int64_t unit_id, const int64_t campaign_id,
                       ObCampaign& campaign);
      int store_campaign(const int64_t unit_id, const int64_t campaign_id,
                         ObCampaign& campaign);

      int get_adgroup(const int64_t unit_id, const int64_t campaign_id,
                      const int64_t adgroup_id, ObAdgroup& adgroup);
      int store_adgroup(const int64_t unit_id, const int64_t campaign_id,
                        const int64_t adgroup_id, ObAdgroup& adgroup);

    private:
      int init_rowkey_meta();
      void init_version_range(common::ObVersionRange& ver_range);
      int read_rowkey_meta(common::ObScanner& scanner);
      int get_cell_value(const common::ObCellInfo& cell_info, 
                         int64_t& value);
      int set_cell_value(common::ObObj& obj, const common::ObObjType type, 
                         const int64_t value);

      int get_database_campaign(const int64_t unit_id, const int64_t campaign_id,
                       ObCampaign& campaign);
      int store_database_campaign(const int64_t unit_id, const int64_t campaign_id,
                         ObCampaign& campaign);
      int read_campaign(common::ObScanner& scanner, ObCampaign& campaign);

      int get_database_adgroup(const int64_t unit_id, const int64_t campaign_id,
                      const int64_t adgroup_id, ObAdgroup& adgroup);
      int store_database_adgroup(const int64_t unit_id, const int64_t campaign_id,
                        const int64_t adgroup_id, ObAdgroup& adgroup);
      int read_adgroup(common::ObScanner& scanner, ObAdgroup& adgroup);


    private:
      static const int64_t CAMPAIGN_HASH_SLOT_NUM = 1024 * 16;
      static const int64_t ADGROUP_HASH_SLOT_NUM = 1024 * 128;

      DISALLOW_COPY_AND_ASSIGN(ObOlapdriveMeta);

      bool inited_;
      client::ObClient& client_;
      ObOlapdriveSchema& olapdrive_schema_;

      ObKeyMeta key_meta_;

      common::hash::ObHashMap<ObCampaignKey, ObCampaign,
        common::hash::NoPthreadDefendMode> campaign_hash_map_;

      common::hash::ObHashMap<ObAdgroupKey, ObAdgroup,
        common::hash::NoPthreadDefendMode> adgroup_hash_map_;
    };

    //utiliy function
    void hex_dump_rowkey(const void* data, const int32_t size, 
                         const bool char_type);
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_META_H
