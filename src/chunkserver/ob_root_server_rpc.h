/*
 *   (C) 2010-2012 Taobao Inc.
 *
 *   Version: 0.1 $date
 *
 *   Authors:
 *      xielun.szd <xielun.szd@taobao.com>
 *
 */

#ifndef OCEANBASE_CHUNKSERVER_ROOT_RPCSTUB_H_
#define OCEANBASE_CHUNKSERVER_ROOT_RPCSTUB_H_

#include "common/ob_schema.h"
#include "common/ob_range2.h"
#include "common/ob_server.h"
#include "common/ob_tablet_info.h"
#include "common/thread_buffer.h"
#include "common/data_buffer.h"
//add liuxiao [secondary index col checksum]20150331
#include "common/ob_column_checksum.h"
//add e

#define rpc_busy_wait(running, ret, rpc) \
    while (running) \
    { \
      ret = (rpc); \
      if (OB_SUCCESS == ret || OB_RESPONSE_TIME_OUT != ret) break; \
      usleep(static_cast<useconds_t>(THE_CHUNK_SERVER.get_param().get_network_time_out())); \
    }

#define rpc_retry_wait(running, retry_times, ret, rpc) \
    while (running && retry_times-- > 0) \
    { \
      ret = (rpc); \
      if (OB_SUCCESS == ret || OB_RESPONSE_TIME_OUT != ret) break; \
      usleep(static_cast<useconds_t>(THE_CHUNK_SERVER.get_config().get_network_timeout())); \
    }


namespace oceanbase
{
  namespace common
  {
    class ObClientManager;
    class ObScanParam;
    class ObScanner;
  }

  namespace chunkserver
  {
    class ObRootServerRpcStub
    {
    public:
      ObRootServerRpcStub();
      virtual ~ObRootServerRpcStub();

    public:
      // warning: rpc_buff should be only used by rpc stub for reset
      int init(const common::ObServer & root_addr, const common::ObClientManager * rpc_frame);

      // get tables schema info through root server rpc call
      //        @timestamp  fetch cmd input param
      //        @schema fetch cmd output schema data
      int fetch_schema(const int64_t timestamp, common::ObSchemaManagerV2 & schema);

      /*
       * Report tablets to RootServer, report finishes at has_more flag is off
       * @param tablets TabletReportInfoList to be reported
       * @param has_more true when there are remaining TabletS to be reported,
       *                 false when all Tablets has been reported
       * @return OB_SUCCESS when successful, otherwise failed
       */
      int report_tablets(const common::ObTabletReportInfoList& tablets,
          const int64_t time_stamp, bool has_more);

      int delete_tablets(const common::ObTabletReportInfoList& tablets);

     /*
       *  notify dest_server to load tablet
       * @param [in] dest_server    migrate destination server
       * @param [in] range          migrated tablet's range
       * @param [in] size           path array size
       * @param [in] tablet_version migrate tablet's data version
       * @param [in] path           sstable file path array
       * @param [in] dest_disk_no   destination disk no
       */
      int dest_load_tablet(
          const common::ObServer &dest_server,
          const common::ObNewRange &range,
          const int32_t dest_disk_no,
          const int64_t tablet_version,
          const uint64_t crc_sum,
          const int64_t size,
          const char (*path)[common::OB_MAX_FILE_NAME_LENGTH]);


      /*
       * notify rootserver migrate tablet is over.
       * @param [in] range migrated tablet's range
       * @param [in] src_server migrate source server
       * @param [in] dest_server migrate destination server
       * @param [in] keep_src =true means copy otherwise move
       * @param [in] tablet_version migrated tablet's data version
       */
      int migrate_over(const common::ObNewRange &range,
          const common::ObServer &src_server,
          const common::ObServer &dest_server,
          const bool keep_src,
          const int64_t tablet_version);

      /*
       * report capacity info of chunkserver for load balance.
       * @param [in] server ip of chunkserver
       * @param [in] capacity total capacity of all disks.
       * @param [in] used total size been used.
       */
      int report_capacity_info(const common::ObServer &server,
          const int64_t capacity, const int64_t used);

      /**
       * @brief get merge delay from rootserver
       * @param interval [out]
       * @return  OB_SUCCESS on success
       */
      int get_merge_delay_interval(int64_t &interval) const;

      /*
       * @brief asynchronous post heartbeat message to rootserver.
       * @param [in] client self address.
       */
      int async_heartbeat(const common::ObServer & client);

      /*
       * @brief source server need to know location of destination server
       * (like disk_no, path) before migrate a tablet.
       * @param [in] occupy_size migrate sstable files occupy size at disk.
       * @param [out] dest_disk_no destination disk no for store migrate sstable files.
       * @param [out] dest_path destination path for store migrate sstable files.
       */
      int get_migrate_dest_location(const int64_t occupy_size,
          int32_t &dest_disk_no, common::ObString &dest_path);

      int get_last_frozen_memtable_version(int64_t &last_version);

      int get_tablet_info(const common::ObSchemaManagerV2& schema,
          const uint64_t table_id, const common::ObNewRange& range,
          common::ObTabletLocation location [],int32_t& size);

      int merge_tablets_over(const common::ObTabletReportInfoList& tablet_list,
        const bool is_merge_succ);

    private:
      int scan(const common::ObServer& server, const int64_t timeout,
               const common::ObScanParam& param, common::ObScanner & result);

      static const int32_t DEFAULT_VERSION = 1;

      // check inner stat
      inline bool check_inner_stat(void) const;

      int get_frame_buffer(common::ObDataBuffer & data_buffer) const;

      // get thread buffer for rpc
      common::ThreadSpecificBuffer::Buffer* get_thread_buffer(void) const;

    private:
      bool init_;                                         // init stat for inner check
      common::ObServer root_server_;                      // root server addr
      const common::ObClientManager * rpc_frame_;         // rpc frame for send request
    };

    // check inner stat
    inline bool ObRootServerRpcStub::check_inner_stat(void) const
    {
      // check server and packet version
      return (init_ && (NULL != rpc_frame_));
    }

  }
}


#endif // OCEANBASE_CHUNKSERVER_ROOT_RPCSTUB_H_
