/*
 *   (C) 2010-2012 Taobao Inc.
 *   
 *   Version: 0.1 $date
 *           
 *   Authors:
 *               
 */

#ifndef OCEANBASE_OBSQL_CLIENT_RPC_H_
#define OCEANBASE_OBSQL_CLIENT_RPC_H_

#include "common/ob_schema.h"
#include "common/ob_server.h"
#include "common/ob_tablet_info.h"
#include "common/thread_buffer.h"
#include "common/data_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet.h"
#include "common/ob_read_common_data.h"
#include "common/thread_buffer.h"
#include "common/ob_scanner.h"
#include "common/ob_statistics.h"
#include "common/ob_mutator.h"
#include "rootserver/ob_chunk_server_manager.h"


class ObClientServerStub 
{
  public:
    static const int64_t FRAME_BUFFER_SIZE = 2*1024*1024L;
    ObClientServerStub();
    virtual ~ObClientServerStub();

  public:
    // warning: rpc_buff should be only used by rpc stub for reset
    int initialize(const oceanbase::common::ObServer & root_server, 
        const oceanbase::common::ObClientManager * rpc_frame);

/*	
	int initialize(const oceanbase::common::ObServer & root_server, 
        const oceanbase::common::ObClientManager * rpc_frame,
        const oceanbase::common::ObServer & update_server,
        const oceanbase::rootserver::ObChunkServerManager & obcsm);
*/
    int cs_scan(const oceanbase::common::ObScanParam& scan_param,
        oceanbase::common::ObScanner& scanner);

    int cs_get(const oceanbase::common::ObGetParam& get_param,
        oceanbase::common::ObScanner& scanner);

    int ups_apply(const oceanbase::common::ObMutator &mutator);

    int get_cs_tablet_image(const oceanbase::common::ObServer & remote_server,
        const int32_t disk_no, 
        oceanbase::common::ObString &image_buf);

    int rs_dump_cs_info(oceanbase::rootserver::ObChunkServerManager &obcsm);

    int fetch_stats(const oceanbase::common::ObServer & remote_server,
		oceanbase::common::ObStatManager &obsm);

    int get_update_server(oceanbase::common::ObServer &update_server);
	
    int fetch_schema(oceanbase::common::ObSchemaManagerV2 &schema);

    int start_merge(const int64_t frozen_memtable_version, const int32_t init_flag);
    int drop_tablets(const int64_t frozen_memtable_version);
    int start_gc(const int32_t reserve);
    
    const oceanbase::common::ObServer & 
      get_root_server() const { return root_server_; }
    const oceanbase::common::ObServer & 
      get_update_server() const { return update_server_; }
    std::vector<oceanbase::common::ObServer> & 
      get_merge_server_list() { return merge_server_list_; }
    std::vector<oceanbase::common::ObServer> & 
      get_chunk_server_list() { return chunk_server_list_; }
    //const oceanbase::rootserver::ObChunkServerManager & 
    //  get_chunk_server_manager() const { return obcs_manager_; }


  private:
    static const int32_t DEFAULT_VERSION = 1;

    // check inner stat
    inline bool check_inner_stat(void) const;

    int get_frame_buffer(oceanbase::common::ObDataBuffer & data_buffer) const;
    int get_cs_and_ms();

  private:
    bool init_;                                             // init stat for inner check
    oceanbase::common::ObServer root_server_;               // root server addr
    oceanbase::common::ObServer update_server_;              
    std::vector<oceanbase::common::ObServer> merge_server_list_;  // merge server addr
    std::vector<oceanbase::common::ObServer> chunk_server_list_;   //chunk servers
    oceanbase::common::ThreadSpecificBuffer frame_buffer_;
    const oceanbase::common::ObClientManager * rpc_frame_;  // rpc frame for send request
};

// check inner stat
inline bool ObClientServerStub::check_inner_stat(void) const
{
  // check server and packet version
  return (init_ && (NULL != rpc_frame_));
}



#endif // OCEANBASE_OBSQL_CLIENT_RPC_H_
