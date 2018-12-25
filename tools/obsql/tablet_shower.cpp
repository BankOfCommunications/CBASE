#include "tablet_shower.h"
#include "common/ob_define.h"
#include "chunkserver/ob_tablet_image.h"
#include "client_rpc.h"

namespace oceanbase
{
  namespace obsql
  {
    using namespace common;
    using namespace chunkserver;

    ObTabletShower::ObTabletShower(ObClientServerStub& rpc_stub, ObServer& server)
      : server_(server), rpc_stub_(rpc_stub)
    {
      disk_nos_.init(disk_no_size, disk_no_array);
    }

/*
    int ObTabletShower::add_servers(std::vector<ObServer>& server_list)
    {
      for (int i = 0; i < server_list.size(); i++)
        servers_.push_back(server_list.at(i));

      return OB_SUCCESS;
    }
*/
    int ObTabletShower::output()
    {
      int ret = OB_SUCCESS;

      if (server_.get_ipv4() != 0)
        ret = output_single_server(server_);
      else
      {
        std::vector<ObServer>& chunk_server_list = rpc_stub_.get_chunk_server_list();
        for (uint32_t i = 0; i < chunk_server_list.size(); i++)
        {
          ret = output_single_server(chunk_server_list.at(i));
          if (ret != OB_SUCCESS)
            break;
        }
      }
      
      return ret;
    }
    int ObTabletShower::output_single_server(ObServer& server)
    {
      int   ret = OB_SUCCESS;
      const int MAX_IP_STRING = 128;
      char  buf[MAX_IP_STRING];

      server.to_string(buf, MAX_IP_STRING);
      fprintf(stderr, "****************************************************************\n");
      fprintf(stderr, "* show tablets of server IP = %s, port = %d\n", buf, server.get_port());
      fprintf(stderr, "****************************************************************\n");

      for (int j = 0; j < disk_nos_.get_array_index(); j++)
      {
       ObString image_buf;
       int64_t pos = 0;
       ObTabletImage image_obj;

       ret = rpc_stub_.get_cs_tablet_image(server, *(disk_nos_.get_base_address() + j), image_buf);
       if (OB_SUCCESS != ret) 
       {
         fprintf(stderr, "disk = %d has no tablets \n ", *(disk_nos_.get_base_address() + j));
         return ret;
       }
       else
       {
         ret = image_obj.deserialize(disk_no_array[j], image_buf.ptr(), image_buf.length(), pos);
       }

       fprintf(stderr, "############################################\n");
       fprintf(stderr, "# disk = %d: \n", *(disk_nos_.get_base_address() + j));
       fprintf(stderr, "############################################\n");

       /* There is no interface for better output, so we invode dump directly */
       //ret = image_obj.dump(true);
       if (ret != OB_SUCCESS)
       {
         fprintf(stderr, "output tablets error! \n");
         return ret;
       }
      }

     return ret;
    }

  }
}

