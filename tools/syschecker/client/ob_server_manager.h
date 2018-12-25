/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_server_manager.h for manage servers. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CLIENT_OB_SERVER_MANAGER_H_
#define OCEANBASE_CLIENT_OB_SERVER_MANAGER_H_

#include "common/ob_server.h"
#include "common/ob_array.h"

namespace oceanbase 
{
  namespace client 
  {
    class ObServerManager
    {
    public:
      ObServerManager();
      ~ObServerManager();

      const common::ObServer& get_root_server() const;
      int set_root_server(const common::ObServer& root_server);

      const common::ObServer& get_update_server() const;
      int set_update_server(const common::ObServer& update_server);

      const common::ObServer& get_random_merge_server() const;
      int add_merge_server(const common::ObServer& merge_server);

      const common::ObServer& get_random_chunk_server() const;
      int add_chunk_server(const common::ObServer& chunk_server);
      
      common::ObArray<common::ObServer> & get_chunk_servers() { return chunk_servers_; }
      common::ObArray<common::ObServer> & get_merge_servers() { return merge_servers_; }
      void set_chunk_servers(const common::ObArray<common::ObServer> & servers) { chunk_servers_ = servers; }
      void set_merge_servers(const common::ObArray<common::ObServer> & servers) { merge_servers_ = servers; }

    private:
      DISALLOW_COPY_AND_ASSIGN(ObServerManager);

      common::ObServer root_server_;
      common::ObServer update_server_;
      common::ObArray<common::ObServer> merge_servers_;
      common::ObArray<common::ObServer> chunk_servers_;

    };
  } // namespace oceanbase::client
} // namespace Oceanbase

#endif //OCEANBASE_CLIENT_OB_SERVER_MANAGER_H_
