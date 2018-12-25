 #include "ob_mysql_resheader_packet.h"
 #include "../ob_mysql_util.h"

 using namespace oceanbase::common;

 namespace oceanbase
 {
   namespace obmysql
   {
     ObMySQLResheaderPacket::ObMySQLResheaderPacket(): field_count_(0)
     {

     }

     ObMySQLResheaderPacket::~ObMySQLResheaderPacket()
     {

     }

     int ObMySQLResheaderPacket::serialize(char* buffer, int64_t len, int64_t& pos)
     {
       int ret = OB_SUCCESS;

       if (NULL == buffer || len <= 0 || pos < 0)
       {
         TBSYS_LOG(ERROR, "invalid argument buffer=%p, len=%ld, pos=%ld",
                   buffer, len, pos);
         ret = OB_INVALID_ARGUMENT;
       }
       else
       {
         if (OB_SUCCESS != (ret = ObMySQLUtil::store_length(buffer, len, field_count_, pos)))
         {
           if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
           {
             TBSYS_LOG(ERROR, "serialize field_count failed, buffer=%p, len=%ld, field_count=%lu, pos=%ld, ret=%d",
                       buffer, len, field_count_, pos, ret);
           }
         }
       }
       return ret;
     }

     uint64_t ObMySQLResheaderPacket::get_serialize_size()
     {
       uint64_t len = 0;
       len += MAX_STORE_LENGTH; // field_count_
       return len;
     }
  }
}
