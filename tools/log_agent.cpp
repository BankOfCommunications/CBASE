#include "common/ob_repeated_log_reader.h"
#include "common/ob_direct_log_reader.h"
#include "common/ob_log_reader.h"
#include "common/utility.h"
#include "common/hash/ob_hashmap.h"
#include "updateserver/ob_ups_mutator.h"
#include "updateserver/ob_ups_utils.h"
#include "updateserver/ob_big_log_writer.h"
#include "common/ob_define.h"
#include "common/ob_version.h"
#include <string>
#include <vector>
#include <sstream>
#include <stdlib.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include "updateserver/ob_ups_table_mgr.h"
#include "sql_builder.h"
#include <getopt.h>
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::updateserver;
using namespace std;


//请求类型
#define PULL_LOG_REQUEST 1000
#define LOGIN_AUTH 1001
#define HEART_BEAT 1002
#define BASE_INFO_REQUEST 1003
#define SCHEMA_REPORT 1004
#define SCHEMA_REQUIRE 1005

#define SCHEMA_APPEND 1006

#define PULL_LOG_CRC_REQUEST 1007

#define PULL_LOG_RESPONSE 2001
#define BASE_INFO_RESPONSE 2002
#define LOGIN_AUTH_RESULT 2003
#define SCHEMA_RESULT 2004
#define PULL_LOG_CRC_RESPONSE 2005

//默认缓冲区大小
static const int64_t MAX_BUFFER_LEN =  4L*(1<<30);//4GB
static int64_t DEFAULT_BUFFER_LEN =  1024 * 1024 * 512;//512MB

//缓冲区大小
static int64_t WRITE_BUFFER_LEN = DEFAULT_BUFFER_LEN + 44;//512MB + 44

//读取日志文件的缓冲区
static const int READ_BUFFER_LEN = 1024 * 1024 * 8;

//存储数据的缓冲区大小
static int64_t DATA_BUFFER_LEN = DEFAULT_BUFFER_LEN;//512MB
static const int SQL_BUFFER_LEN  = 100*10000;//100万行数据
//日志文件大小
static const int MAX_LOG_SIZE = 1024 * 1024 * 256;//256MB


//版本号
#define VERSION_N 171717

//网络状态
#define CONNECTED 0

#define DISCONNECTED -1

//flag
#define BUFFER_OVERFLOW -6
#define AGENT_ERROR -5
#define SCHEMA_INVAILD -4
#define SCHEMA_SWITCH -3
#define SEQ_INVALID -2
#define LOG_END -1
#define NORMAL 0
#define LOG_SWITCH 101
#define BIG_LOG_SWITCH 90000

//################################华丽的分割线 1#########################################

//拉取请求
struct pull_request
{
    int32_t sno;		//序列号

    int32_t type;		//类型

    char* log_name;     //日志文件名

    int64_t seq;		//日志号

    int32_t version;	//版本

    int32_t count;		//数量

    char* extend;       //额外字段

    int32_t extend_size;//额外字段大小
};

//返回的结果
struct pull_response
{
    int32_t sno;		//序列号

    int32_t type;		//类型

    int32_t version;	//版本

    int32_t count;		//SQL条数

    int32_t flag;		//切换日志

    int64_t seq;		//本次最后读取的日志号

    int64_t real_seq;	//本次最后读取有效的mutator

    int64_t body_size;  //结果长度

    char* body;         //结果，可能多条SQL的内容，格式为|Length:4|SEQ:4|Data:N|...|
};


class LogAgent
{
    public:
        LogAgent();
        int start(const int argc, char *argv[]);
        int reconnect();//重连
        int balanceBuffer();//动态缓存succ_num=0 over_flow buf increase;=1 buf default
        DISALLOW_COPY_AND_ASSIGN(LogAgent);
    private:
            int succ_num;
            CommonSchemaManagerWrapper schema;

            int schema_flag;//0 表示正常  -1 表示为空

            //连接标志位
            int connected_flag;
            //socket句柄
            int sockfd;
            SqlBuilder builder;

            int current_file_id;

            char* socket_send_buf;

            char* sql_buf;

            char master_ip[OB_IP_STR_BUFF];
            int32_t master_port;
            char ups_log_dir[OB_MAX_FILE_NAME_LENGTH];

            tbsys::CThreadCond cond_;
            ObBigLogWriter big_log_writer;//>2M log

    private:
            //序列化对象
            int build_response(char* buf, const pull_response response, int64_t &size);

            //重构schema对象
            int rebuild_schema(LogCommand cmd, uint64_t seq, const char* log_data, int64_t data_len);

            //追加schema信息
            int schema_append(int fd, char* log_dir, pull_request request);
            //重构SQL
            int rebuild_sql(uint64_t seq, const char* log_data, int64_t data_len, vector<string> &sqls);

            int get_checksum(uint64_t seq, const char* log_data, int64_t data_len, int64_t &data_checksum_before, int64_t &data_checksum_after);

            //发送心跳
            static void* send_heart_beat(void* args);

            //发送注册信息
            int send_login_auth_request(int sockfd);

            //更新schema信息
            int pull_schema(int sockfd);

            //把SQL语句序列化
            int encode_data(char *buf, const int64_t buf_len, int64_t& pos, const char *sql, int64_t seq, int32_t index);

            //序列化头部数据
            int encode_data_header(char *buf, const int64_t buf_len, int64_t& pos, int64_t seq, int64_t checksum_before, int64_t checksum_after, int32_t count);

            //连接服务器
            int connect_server(int &fd ,const char* server_ip, uint32_t port);

            //解析收到的请求
            int parse_request(char* buf, int64_t data_len, pull_request &request);

            //处理拉取SQL的请求
            int handler_pull_request(int fd, char* log_dir, pull_request request);

            //处理拉取日志CRC校验码的请求
            int handler_pull_crc_request(int fd, char* log_dir, pull_request request);

            //读取n个字节
            ssize_t readn(int fd, void *vptr, size_t n);

            //写入n个字节
            ssize_t writen(int fd,const void *vptr, size_t n);

            //收取一个数据包
            int receive_packet(int fd, pull_request &request);

            //发送schema信息到Server(Server存储Schema信息，在Agent启动的时候可以获取)
            int send_schema(int fd, uint64_t seq ,CommonSchemaManagerWrapper &schema);

            void parse_cmd_line(const int argc,  char *const argv[]);

            void print_usage(const char *prog_name);

            void print_version();
      public:
            //发送一个数据包
            int send_packet(int fd, const pull_response response);

};



//################################华丽的分割线 2#########################################


LogAgent::LogAgent()
{
    succ_num=100;
    schema_flag = -1;
    sockfd = -1;
    connected_flag = DISCONNECTED;
    socket_send_buf = new char[WRITE_BUFFER_LEN];

    memset(socket_send_buf, 0, WRITE_BUFFER_LEN);//置为0

    sql_buf = new char[DATA_BUFFER_LEN];
    memset(sql_buf, 0, DATA_BUFFER_LEN);//置为0
    big_log_writer.init();
}

int LogAgent::reconnect()
{
    int ret=OB_SUCCESS;
    schema_flag = -1;
    sockfd = -1;
    connected_flag = DISCONNECTED;
    return ret;
}

int LogAgent::balanceBuffer()//succ_num:=0OB_MEM_OVERFLOW buf increase;=1 buf set default
{
    int ret=OB_SUCCESS;

    if(this->succ_num==100 && DATA_BUFFER_LEN != DEFAULT_BUFFER_LEN) //buf第一次正常，
    {
        WRITE_BUFFER_LEN = DEFAULT_BUFFER_LEN + 44;//512MB + 44
        DATA_BUFFER_LEN = DEFAULT_BUFFER_LEN;//512MB
    }
    else if(this->succ_num>0)//buf 正常
    {
        if(this->succ_num>1000) this->succ_num=200;
        return ret;
    }
    else if(this->succ_num==0)//buf不够用
    {
        WRITE_BUFFER_LEN += DEFAULT_BUFFER_LEN;//+512MB
        DATA_BUFFER_LEN  += DEFAULT_BUFFER_LEN;//+512MB
    }
    else
    {
        TBSYS_LOG(ERROR, "\033[33m#balanceBuffer,succ_num=%d#\033[0m",this->succ_num);
        exit(0);
    }

    if(DATA_BUFFER_LEN>MAX_BUFFER_LEN)// > 4GB
    {
        TBSYS_LOG(ERROR, "\033[33m#balanceBuffer too large,DATA_BUFFER_LEN=%ld,MAX_BUFFER_LEN=%ld#\033[0m",
                  DATA_BUFFER_LEN,MAX_BUFFER_LEN);
        exit(0);
    }

    TBSYS_LOG(INFO, "\033[33m#Reset LogAgent buffer,current_buffer=%ld#\033[0m",DATA_BUFFER_LEN);
    delete [] this->socket_send_buf;
    this->socket_send_buf = new char[WRITE_BUFFER_LEN];
    memset(this->socket_send_buf, 0, WRITE_BUFFER_LEN);//置为0

    delete [] this->sql_buf;
    this->sql_buf = new char[DATA_BUFFER_LEN];
    memset(this->sql_buf, 0, DATA_BUFFER_LEN);//置为0
    return ret;
}

int LogAgent::start(const int argc, char *argv[])
{
    //解析参数
    parse_cmd_line(argc,argv);
    //赋值参数
    char* log_dir = ups_log_dir;
    char* remote_ip = master_ip;
    uint32_t remote_port = (uint32_t)master_port;
    //连接server
    if(OB_SUCCESS != connect_server(sockfd, remote_ip, remote_port))
    {
        return -1;
    }
    else
    {
       TBSYS_LOG(INFO, "\033[33m#connect server %s:%d success#\n\033[0m",remote_ip, remote_port);
    }
    while(true)
    {
        pull_request request;
        if(OB_SUCCESS != receive_packet(sockfd, request))
        {
            connected_flag = DISCONNECTED;
            //断开连接了，重连服务器
            while (true)
            {
               TBSYS_LOG(INFO, "\033[31m#reconnect server %s:%d ...#\033[0m\n",remote_ip, remote_port);
                sockfd = -1;
                if(OB_SUCCESS == connect_server(sockfd, remote_ip, remote_port))
                {
                  TBSYS_LOG(INFO, "\033[33m#reconnect server %s:%d success#\033[0m\n",remote_ip, remote_port);
                   break;
                }
                sleep(10);
            }
        }
        else
        {
           TBSYS_LOG(INFO, "\033[36mreceive request:(sno=%d, type=%d, log_name=%s, seq=%ld, count=%d)\033[0m\n" ,request.sno, request.type, request.log_name, request.seq, request.count);

            if (request.type == PULL_LOG_REQUEST)
            {
                //处理拉取日志请求
                if (OB_SUCCESS != handler_pull_request(sockfd, log_dir, request))// process_reqeust_multi(sockfd, log_dir, request, tools)
                {
                    TBSYS_LOG(ERROR, "handler_pull_request error!\n");
                }
                //fprintf(stdout, "###################sno end#################\n\n");
            }
            else if (request.type == PULL_LOG_CRC_REQUEST)
            {
                //处理拉取日志校验码请求
                if (OB_SUCCESS != handler_pull_crc_request(sockfd, log_dir, request))
                {
                    TBSYS_LOG(ERROR, "handler_pull_crc_request error!\n");
                }
            }
            else if (request.type == SCHEMA_RESULT)//server主动推schema信息，一般是主备UPS切换的时候
            {
                if(request.seq == -1)//schema为空
                {
                   schema_flag = -1;
                   TBSYS_LOG(INFO, "#receive empty schema#\n");
                }
                else
                {
                    schema.reset();
                    int64_t pos = 0;
                    if (OB_SUCCESS != schema.deserialize(request.extend, request.extend_size, pos))
                    {
                       TBSYS_LOG(ERROR, "ObSchemaManagerWrapper deserialize error\n");
                    }
                    else
                    {
                       TBSYS_LOG(INFO, "\033[33mreceive schema success, size:%ldbyte\033[0m\n", pos);
                       schema_flag = 1;
                    }
                }
            }
            else if(request.type == SCHEMA_APPEND)
            {
                if(OB_SUCCESS != schema_append(sockfd, log_dir,request))
                {
                    TBSYS_LOG(ERROR, "schema append error\n");
                }
                else
                {
                    TBSYS_LOG(INFO, "schema append success");
                }
            }
            else
            {
                TBSYS_LOG(ERROR, "request type error!\n");
                exit(0);
            }

            //释放内存
            if(request.log_name != NULL)
            {
                delete[] request.log_name;
            }
            if(request.extend != NULL)
            {
                delete[] request.extend;
            }
        }
    }
}

//构建返回数据
int LogAgent::build_response(char* buf, const pull_response response, int64_t &size)
{
    int ret = OB_SUCCESS;
    int64_t pos = 4;
    int header_size = 44;//4 + 4 + 4 + 4 + 4 + 8 + 8 + 8
    if(header_size + response.body_size > WRITE_BUFFER_LEN)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR,  "buffer size not enough, fixed size=%ld, need size=%ld\n", WRITE_BUFFER_LEN, header_size + response.body_size);
    }
    if (OB_SUCCESS != (ret = serialization::encode_i32(buf, WRITE_BUFFER_LEN, pos, response.sno)))
    {
      TBSYS_LOG(ERROR,  "fail to serialize sno. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = serialization::encode_i32(buf, WRITE_BUFFER_LEN, pos, response.type)))
    {
      TBSYS_LOG(ERROR,  "fail to serialize type. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = serialization::encode_i32(buf, WRITE_BUFFER_LEN, pos, response.version)))
    {
       TBSYS_LOG(ERROR, "fail to serialize version. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = serialization::encode_i32(buf, WRITE_BUFFER_LEN, pos, response.count)))
    {
       TBSYS_LOG(ERROR, "fail to serialize count. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = serialization::encode_i32(buf, WRITE_BUFFER_LEN, pos, response.flag)))
    {
       TBSYS_LOG(ERROR, "fail to serialize flag. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, WRITE_BUFFER_LEN, pos, response.seq)))
    {
       TBSYS_LOG(ERROR, "fail to serialize size. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, WRITE_BUFFER_LEN, pos, response.real_seq)))
    {
       TBSYS_LOG(ERROR, "fail to serialize size. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, WRITE_BUFFER_LEN, pos, response.body_size)))
    {
        TBSYS_LOG(ERROR, "fail to serialize len. ret=%d", ret);
    }
    else
    {
        if(response.body != NULL && response.body_size > 0)
        {
            memcpy(buf + pos, response.body, response.body_size);
            pos += response.body_size;
        }
        int64_t temp_pos = 0;
        if (OB_SUCCESS != (ret = serialization::encode_i32(buf, WRITE_BUFFER_LEN, temp_pos, (int32_t)(pos - 4))))//first 4 byte wite length
        {
            TBSYS_LOG(ERROR, "fail to serialize len. ret=%d", ret);
        }
        else
        {
            size = pos;
        }
    }
    return ret;
}

/**
 * @brief 重构schema信息
 * @param cmd
 * @param seq
 * @param log_data
 * @param data_len
 * @return
 */
int LogAgent::rebuild_schema(LogCommand cmd, uint64_t seq, const char* log_data, int64_t data_len)
{
    UNUSED(seq);
    int ret = OB_SUCCESS;
    ObSchemaMutator schema_mutator;
    int64_t pos = 0;
    switch(cmd)
    {
        case OB_UPS_SWITCH_SCHEMA://Schema全量
            schema.reset();
            if (OB_SUCCESS != (ret = schema.deserialize(log_data, data_len, pos)))
            {
               TBSYS_LOG(ERROR, "ObSchemaManagerWrapper deserialize error[ret=%d log_data=%p data_len=%ld]\n",
                        ret, log_data, data_len);
            }
            else
            {
               schema_flag = 0;
               TBSYS_LOG(INFO, "#ObSchemaManagerWrapper deserialize success#\n");
            }
            break;
        case OB_UPS_WRITE_SCHEMA_NEXT:
           TBSYS_LOG(ERROR, "\033[31m#OB_UPS_WRITE_SCHEMA_NEXT not process!#\033[0m\n");
          break;
        case OB_UPS_SWITCH_SCHEMA_NEXT:
           TBSYS_LOG(ERROR, "\033[31m#OB_UPS_SWITCH_SCHEMA_NEXT not process!#\033[0m\n");
          break;
        case OB_UPS_SWITCH_SCHEMA_MUTATOR://Schema增量

            if (OB_SUCCESS != (ret = schema_mutator.deserialize(log_data, data_len, pos)))
            {
              TBSYS_LOG(ERROR, "ObSchemaMutator deserialize error[ret=%d log_data=%p data_len=%ld]\n",
                      ret, log_data, data_len);
            }
            else if(schema.get_version()==schema_mutator.get_end_version())
            {
                TBSYS_LOG(INFO, "#schema_mutator has already been applyed,schema version=%ld, schema_mutator end version=%ld#\n",
                          schema.get_version(),schema_mutator.get_end_version());
            }
            else if(OB_SUCCESS != (ret = schema.get_impl_ref().apply_schema_mutator(schema_mutator)))
            {
                TBSYS_LOG(ERROR, "#apply_schema_mutator error ret=%d, schema version=%ld, schema_mutator start version=%ld, end version=%ld#\n",
                          ret,schema.get_version(),schema_mutator.get_start_version(), schema_mutator.get_end_version());
            }
            else
            {
                TBSYS_LOG(INFO, "#apply_schema_mutator succ, schema version=%ld, schema_mutator start version=%ld, end version=%ld#\n",
                          schema.get_version(),schema_mutator.get_start_version(), schema_mutator.get_end_version());
            }
            break;

        default:
            ret = OB_ERROR;
            break;
    }

    if(OB_SUCCESS == ret)
    {
        //发送schema信息到服务器
        ret = send_schema(sockfd, seq, schema);
    }
    else
    {
        //schema 异常，重新从capture拉取最新schema
        pull_schema(sockfd);
    }

    return ret;
}
int LogAgent::schema_append(int fd, char *log_dir, pull_request request)
{
    int ret = OB_SUCCESS;

    //日志读取组件
    ObLogReader log_reader;
    ObRepeatedLogReader reader;
    ObLogCursor end_cursor;

    LogCommand cmd;
    uint64_t log_seq = 0;
    char* log_data = NULL;
    int64_t data_len = 0;

    int log_name = atoi(request.log_name);//请求的文件名
    uint64_t last_log_seq = (const uint64_t)request.seq;//请求中上一次的日志号
    int count = 0;
    //响应
    pull_response response;
    response.sno = request.sno;
    response.type = PULL_LOG_RESPONSE;
    response.version = VERSION_N;
    response.seq = 0;
    response.real_seq = 0;
    response.flag = NORMAL;
    response.count = 0;
    response.body = (char*)"agent";
    response.body_size = (int64_t)strlen(response.body);

    //读取日志数据
    if (OB_SUCCESS != (ret = log_reader.init(&reader, log_dir, log_name, 0, false)))//从文件头开始
    {
        TBSYS_LOG(ERROR, "ObLogReader init error[err=%d]\n", ret);
    }
    TBSYS_LOG(INFO, "last_log_seq = %ld , log_seq = %ld\n", last_log_seq, log_seq);
    while (last_log_seq > log_seq && OB_SUCCESS == ret)
    {
        count++;
        if (OB_SUCCESS != (ret = log_reader.read_log(cmd, log_seq, log_data, data_len)) &&
            OB_FILE_NOT_EXIST != ret && OB_READ_NOTHING != ret && OB_LAST_LOG_RUINNED != ret)
        {
            TBSYS_LOG(ERROR, "ObLogReader read error[ret=%d]\n", ret);
        }
        else if (OB_LAST_LOG_RUINNED == ret)
        {
            TBSYS_LOG(ERROR, "last_log[%s] broken!\n", end_cursor.to_str());
        }
        else if (OB_FILE_NOT_EXIST == ret)
        {
            TBSYS_LOG(ERROR, "log file not exist\n");
        }
        else if (OB_READ_NOTHING == ret)
        {
            //TBSYS_LOG(ERROR, "\033[31mwaiting log data\033[0m\n");
        }
        else if (OB_SUCCESS != (ret = log_reader.get_next_cursor(end_cursor)))
        {
            TBSYS_LOG(ERROR, "log_reader.get_cursor()=>%d\n",  ret);
        }
        else
        {

            //current_file_id = log_reader.get_cur_log_file_id();
            current_file_id = log_name;

            //日志
            if (OB_LOG_UPS_MUTATOR == cmd)
            {
               TBSYS_LOG(INFO, "\033[35mread log => SEQ: %lu Payload Length: %ld TYPE: %s\033[0m\n", log_seq, data_len, "OB_LOG_UPS_MUTATOR");
            }
            //日志切换
            else if (OB_LOG_SWITCH_LOG == cmd)
            {
               TBSYS_LOG(INFO, "\033[35mread log => SEQ: %lu Payload Length: %ld TYPE: %s\033[0m\n", log_seq, data_len, "OB_LOG_SWITCH_LOG");
               response.flag = LOG_SWITCH;
               break;
            }
            //schema相关
            else if (OB_UPS_SWITCH_SCHEMA == cmd || OB_UPS_SWITCH_SCHEMA_MUTATOR == cmd || OB_UPS_SWITCH_SCHEMA_NEXT == cmd || OB_UPS_WRITE_SCHEMA_NEXT == cmd)
            {
               TBSYS_LOG(INFO, "\033[35mread log => SEQ: %lu Payload Length: %ld TYPE: %s\033[0m\n", log_seq, data_len, "OB_SCHEMA");
               ret = rebuild_schema(cmd, log_seq, log_data, data_len);
            }
            else if (OB_LOG_NOP == cmd)
            {
                //todo nothing
            }
        }
    }

    if (OB_FILE_NOT_EXIST == ret)
    {
        response.flag = LOG_END;
        ret = OB_SUCCESS;
    }
    else if (OB_READ_NOTHING == ret)
    {
        response.flag = LOG_END;
        ret = OB_SUCCESS;
    }

    //设置读取到的最后一条日志号
    response.seq = log_seq;

    //设置读取的日志数量
    response.count = 0;//count;
    //响应数据
    if(OB_SUCCESS == ret)
    {
        if (OB_SUCCESS != (ret = send_packet(fd, response)))
        {
            TBSYS_LOG(ERROR, "send response error, ret=%d\n", ret);
        }
        else
        {
           TBSYS_LOG(INFO, "\033[33msend schema append response success, last read seq=%lu, sno=%d, read count=%d\033[0m\n", log_seq, request.sno,count);
        }
    }

    //关闭日志文件
    if (OB_SUCCESS == ret && reader.is_opened())
    {
      ret = reader.close();
      if(OB_SUCCESS != ret)
      {
          TBSYS_LOG(ERROR, "log reader close error!\n");
      }
    }

    return ret;
}

//重新生成SQL
int LogAgent::rebuild_sql(uint64_t seq, const char* log_data, int64_t data_len, vector<string> &sqls)
{
    int ret = OB_SUCCESS;
    if(seq == 1)
    {
       TBSYS_LOG(INFO, "fuck this log is bad.\n");
       //ret = OB_INVALID_ERROR;
    }
    else
    {
        ObUpsMutator mut;
        int64_t pos = 0;
        ret = mut.deserialize(log_data, data_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "Error occured when deserializing ObUpsMutator.\n");
        }
        else
        {
            if (mut.is_freeze_memtable())
            {
              //Freeze Operation
                TBSYS_LOG(WARN, "mutator is freeze memtable.\n");
            }
            else if (mut.is_drop_memtable())
            {
              //Drop Operation
                TBSYS_LOG(WARN, "mutator is drop memtable.\n");
            }
            else
            {
                //生成SQL
                if(schema.get_version() > 0 && schema_flag == 0)
                {
                    ret = builder.ups_mutator_to_sql(seq, mut, sqls, &schema);
                    //TBSYS_LOG(ERROR, "SEQ: %lu\n",seq);

                }
                else
                {
                    ret = OB_SCHEMA_ERROR;
                    TBSYS_LOG(WARN, "No Schema.\n");
                }
            }

        }
    }
    return ret;
}

int LogAgent::get_checksum(uint64_t seq, const char* log_data, int64_t data_len, int64_t &data_checksum_before, int64_t &data_checksum_after)
{
    int ret = OB_SUCCESS;
    UNUSED(seq);
    ObUpsMutator mut;
    int64_t pos = 0;
    ret = mut.deserialize(log_data, data_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "Error occured when deserializing ObUpsMutator.\n");
    }
    else
    {
       data_checksum_before = mut.get_memtable_checksum_before_mutate();
       data_checksum_after = mut.get_memtable_checksum_after_mutate();
    }
    return ret;
}

//发送心跳信息
void* LogAgent::send_heart_beat(void* args)
{
    int ret = OB_SUCCESS;
    UNUSED(args);
    LogAgent* agent = (LogAgent*)args;
    int fd = agent->sockfd;
    while(true && agent->connected_flag == CONNECTED)
    {
        pull_response response;
        response.sno = 0;
        response.type = HEART_BEAT;
        response.seq = 0;
        response.real_seq = 0;
        response.version = VERSION_N;
        response.count = 0;
        response.flag = 0;
        response.body = (char*)"hello";
        response.body_size = (int64_t)strlen(response.body);
        if(OB_SUCCESS == (ret = agent->send_packet(fd, response)))
        {
          TBSYS_LOG(INFO, "\033[33m#send heart beat...#\033[0m\n");
           sleep(30);
        }
        else
        {
           TBSYS_LOG(ERROR, "\033[33m#send heart beat error, ret=%d#\033[0m\n", ret);
           break;
        }
    }
    TBSYS_LOG(INFO, "\033[33m#send heart beat thread end#\033[0m\n");
    return NULL;

}


//序列化SQL
int LogAgent::encode_data(char *buf, const int64_t buf_len, int64_t& pos, const char *sql, int64_t seq, int32_t index)
{
    //index:4 | length:4 | seq:8 | sql_data:n
    int ret = OB_SUCCESS;
    int32_t length = (int32_t)strlen(sql);
    if(buf_len - pos < (length + 12))
    {
      ret = OB_MEM_OVERFLOW;
      TBSYS_LOG(WARN,  "buffer size not enough, fixed size=%ld, need size=%d, pos=%ld\n", buf_len, length, pos);
    }
    else if (OB_SUCCESS != (ret = serialization::encode_i32(buf, buf_len, pos, index)))
    {
      TBSYS_LOG(ERROR,  "fail to serialize sql length. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = serialization::encode_i32(buf, buf_len, pos, length)))
    {
      TBSYS_LOG(ERROR,  "fail to serialize sql length. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, seq)))
    {
      TBSYS_LOG(ERROR,  "fail to serialize seq. ret=%d", ret);
    }
    else
    {
        memcpy(buf + pos, sql, length);
        pos += length;
    }
    return ret;
}

int LogAgent::encode_data_header(char *buf, const int64_t buf_len, int64_t& pos, int64_t seq, int64_t checksum_before, int64_t checksum_after, int32_t count)
{
    //seq:8 | check_sum_before:8 | check_sum_after:8 | sql_count:4
    int ret = OB_SUCCESS;
    if(buf_len - pos < 28)
    {
      ret = OB_MEM_OVERFLOW;
      TBSYS_LOG(ERROR,  "buffer size not enough, fixed size=%ld, pos=%ld\n", buf_len, pos);
    }
    else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, seq)))
    {
      TBSYS_LOG(ERROR,  "fail to serialize seq. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, checksum_before)))
    {
      TBSYS_LOG(ERROR,  "fail to serialize checksum_before. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, checksum_after)))
    {
      TBSYS_LOG(ERROR,  "fail to serialize checksum_after. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = serialization::encode_i32(buf, buf_len, pos, count)))
    {
      TBSYS_LOG(ERROR,  "fail to serialize count. ret=%d", ret);
    }
    return ret;
}


//向server注册
int LogAgent::send_login_auth_request(int sockfd)
{
    int ret = OB_SUCCESS;
    //auth
    pull_response auth;
    auth.sno = 0;
    auth.type = LOGIN_AUTH;
    auth.seq = 0;
    auth.real_seq = 0;
    auth.version = VERSION_N;
    auth.count = 0;
    auth.flag = 0;
    auth.body = (char*)"agent";
    auth.body_size = (int64_t)strlen(auth.body);

    pull_request auth_result;
    if(OB_SUCCESS != (ret = send_packet(sockfd, auth)))
    {
        TBSYS_LOG(ERROR, "@send auth packet error@\n");
    }
    else
    {
        TBSYS_LOG(INFO, "#send auth packet success#\n");
        if (OB_SUCCESS != (ret = receive_packet(sockfd, auth_result)))
        {
            TBSYS_LOG(ERROR, "@receive auth result error@\n");
        }
        else
        {
           TBSYS_LOG(INFO, "#receive auth result#\n");
        }

        //释放内存
        if(auth_result.log_name != NULL)
        {
            delete[] auth_result.log_name;
        }
        if(auth_result.extend != NULL)
        {
            delete[] auth_result.extend;
        }
    }
    return ret;
}

int LogAgent::pull_schema(int sockfd)
{
    int ret = OB_SUCCESS;
    //schema
    pull_response schema_req;
    schema_req.sno = 0;
    schema_req.type = SCHEMA_REQUIRE;
    schema_req.seq = 0;
    schema_req.real_seq = 0;
    schema_req.version = VERSION_N;
    schema_req.count = 0;
    schema_req.flag = 0;
    schema_req.body = (char*)"schema";//无用信息
    schema_req.body_size = (int64_t)strlen(schema_req.body);

    pull_request schema_result;
    if(OB_SUCCESS != (ret = send_packet(sockfd, schema_req)))
    {
        TBSYS_LOG(ERROR, "@send schema require packet error@\n");
    }
    else
    {
        TBSYS_LOG(INFO, "#send require packet success#\n");

        if (OB_SUCCESS != (ret = receive_packet(sockfd, schema_result)))
        {
            TBSYS_LOG(ERROR, "@receive schema result error@\n");
        }
        else
        {
            if(schema_result.seq == -1)//schema为空
            {
               schema_flag = -1;
               schema.reset();
               TBSYS_LOG(INFO, "#pull empty schema#\n");
            }
            else
            {
                schema.reset();
                int64_t pos = 0;
                if (OB_SUCCESS != schema.deserialize(schema_result.extend, schema_result.extend_size, pos))
                {
                   TBSYS_LOG(ERROR, "ObSchemaManagerWrapper deserialize error\n");
                }
                else
                {
                   schema_flag = 0;
                   TBSYS_LOG(INFO, "\033[33mpull schema success, size:%ldbyte\033[0m\n", pos);
                }
            }
        }

        //释放内存
        if(schema_result.log_name != NULL)
        {
            delete[] schema_result.log_name;
        }
        if(schema_result.extend != NULL)
        {
            delete[] schema_result.extend;
        }

    }
    return ret;
}

int LogAgent::connect_server(int &fd ,const char* server_ip, uint32_t port)
{
    int ret = OB_SUCCESS;
    pthread_t tid;//线程id

    struct sockaddr_in server_addr;//服务器地址
    bzero(&server_addr,sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    inet_pton(AF_INET,server_ip, &server_addr.sin_addr);

    server_addr.sin_port = htons(port);//bug , warning

    //构建socket
    fd = socket(AF_INET,SOCK_STREAM, 0);
    if(-1 == fd)
    {
        TBSYS_LOG(ERROR, "@socket fail@\n");
        ret = OB_ERROR;
    }
    //开始连接服务器
    else if(-1 == connect(fd,(struct sockaddr *)(&server_addr),sizeof(server_addr)))
    {
        TBSYS_LOG(WARN, "@connect server failed@\n");
        ret = OB_ERROR;
    }
    else
    {
        connected_flag = CONNECTED;//设置已连接标志位
    }

    if(OB_SUCCESS == ret)
    {
        //发送验证信息
        if(OB_SUCCESS != (ret = send_login_auth_request(fd)))
        {
            TBSYS_LOG(ERROR, "@auth error@\n");
        }

        //更新schema信息
        if(OB_SUCCESS != (ret = pull_schema(fd)))
        {
            TBSYS_LOG(ERROR, "@update schema error@\n");
        }

        //启动发送心跳包的线程
        else if(0 != pthread_create(&tid, NULL, send_heart_beat, (void*)this))
        {
            TBSYS_LOG(ERROR, "@start thread error@\n");
            ret = OB_ERROR;
        }
    }
    return ret;
}

int LogAgent::handler_pull_request(int fd, char* log_dir, pull_request request)
{
    int ret = OB_SUCCESS;

    //2Mlog
    bool is_big_log=false;
    int  big_log_switch_num=0;
    int  big_log_num=0;

    //日志读取组件
    ObLogReader log_reader;
    ObRepeatedLogReader reader;
    ObLogCursor end_cursor;

    LogCommand cmd;
    uint64_t log_seq = 0;
    char* log_data = NULL;
    int64_t data_len = 0;

    //buf 自动调整
    this->balanceBuffer();

    int log_name = atoi(request.log_name);//请求的文件名
    uint64_t last_log_seq = (const uint64_t)request.seq;//请求中上一次的日志号

    //拉取的日志数
    int count = request.count;
    int64_t limit_size = DATA_BUFFER_LEN;
    //响应
    pull_response response;
    response.sno = request.sno;
    response.type = PULL_LOG_RESPONSE;
    response.version = VERSION_N;
    response.seq = last_log_seq;
    response.real_seq = 0;
    response.flag = 0;
    response.count = 0;
    response.body = sql_buf;
    response.body_size = 0;
    int32_t cell_info_count = 0;
    int64_t old_pos = response.body_size;
    //读取日志数据
    if (OB_SUCCESS != (ret = log_reader.init(&reader, log_dir, log_name, 0, false)))
    {
        TBSYS_LOG(ERROR, "ObLogReader init error[err=%d]\n", ret);
    }

    //init log reader,
    if(last_log_seq > 0)
    {
        while (OB_SUCCESS == (ret = log_reader.read_log(cmd, log_seq, log_data, data_len)))
        {
            if(log_seq>=last_log_seq)
                break;
        }
        if(log_seq!=last_log_seq)
        {
            TBSYS_LOG(ERROR, "ObLogReader init error, last_log_seq[%lu] cur_log_seq[%lu]\n",
                      last_log_seq,log_seq);
            ret=OB_INVALID_LOG;
        }
    }
    big_log_writer.reset_Last_tranID();//初始化big log,last tran id
    while (big_log_num < 1 && count > 0 && OB_SUCCESS == ret )
    {
        if(is_big_log==false && cell_info_count > SQL_BUFFER_LEN)
        {
            break;
        }

        if (OB_SUCCESS != (ret = log_reader.read_log(cmd, log_seq, log_data, data_len)) &&
            OB_FILE_NOT_EXIST != ret && OB_READ_NOTHING != ret && OB_LAST_LOG_RUINNED != ret)
        {
            TBSYS_LOG(ERROR, "ObLogReader read error[ret=%d]\n", ret);
        }
        else if (OB_LAST_LOG_RUINNED == ret)
        {
            TBSYS_LOG(ERROR, "last_log[%s] broken!\n", end_cursor.to_str());
        }
        else if (OB_FILE_NOT_EXIST == ret)
        {
            TBSYS_LOG(WARN, "log file not exist\n");
        }
        else if (OB_READ_NOTHING == ret)
        {
            //TBSYS_LOG(ERROR, "\033[31mwaiting log data\033[0m\n");
            //设置读取到的最后一条日志号
            if(!is_big_log)
            {
                response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
            }
        }
        else if (OB_SUCCESS != (ret = log_reader.get_next_cursor(end_cursor)))
        {
            TBSYS_LOG(ERROR, "log_reader.get_cursor()=>%d\n",  ret);
        }
        else
        {
            current_file_id = log_name;
            //超过2M日志，单独处理
            //普通日志
            if (OB_LOG_UPS_MUTATOR == cmd || OB_UPS_BIG_LOG_DATA == cmd)
            {
                //超过2M日志，单独处理
                if( OB_UPS_BIG_LOG_DATA == cmd )
                {
                  is_big_log=true;
                  bool is_all_completed=false;
                  if(OB_SUCCESS!=(ret=big_log_writer.package_big_log(log_data,data_len,is_all_completed)))
                  {
                    TBSYS_LOG(ERROR, "SEQ:%lu,package_big_log fail,ret=%d",log_seq,ret);
                  }
                  else if(is_all_completed)
                  {
                    int32_t temp_len;
                    big_log_writer.get_buffer(log_data,temp_len);
                    data_len=(int64_t)temp_len;
                    is_big_log=false;
                    big_log_num++;
                    TBSYS_LOG(DEBUG, "package_big_log completed SEQ:%lu ,len=%lu",log_seq,data_len);
                    if(big_log_switch_num>0)
                    {
                        response.flag=BIG_LOG_SWITCH;
                        response.flag+=big_log_switch_num;
                    }
                  }
                  else
                  {
                      continue;
                  }
                }

                //TBSYS_LOG(INFO, "\033[35mread log => SEQ: %lu Payload Length: %ld TYPE: %s\033[0m\n", log_seq, data_len, "OB_LOG_UPS_MUTATOR");
                //解析sql
                vector<string> sqls;
                int64_t data_checksum_before;
                int64_t data_checksum_after;
                old_pos = response.body_size;
                if (OB_SUCCESS != (ret = rebuild_sql(log_seq, log_data, data_len, sqls)))
                {
                    TBSYS_LOG(ERROR, "rebuild_sql error, ret=%d\n", ret);
                }
                else if(sqls.size() <= 0)
                {
                    TBSYS_LOG(DEBUG, "rebuild_sql maybe error, count:%d\n", (int32_t)sqls.size());
                }
                else if(sqls.size() > 0 && OB_SUCCESS != (ret = get_checksum(log_seq, log_data, data_len, data_checksum_before,data_checksum_after)))
                {
                    TBSYS_LOG(ERROR, "get_checksum error, ret=%d\n", ret);
                }
                else if(sqls.size() > 0 && OB_SUCCESS != (ret = encode_data_header(response.body, limit_size, response.body_size,log_seq,data_checksum_before,data_checksum_after,(int32_t)sqls.size())))
                {
                    TBSYS_LOG(ERROR, "encode_data_header error, ret=%d\n", ret);
                }
                else
                {
                    for (vector<string>::iterator iter = sqls.begin(); iter != sqls.end(); iter++)
                    {
                        string sql = *iter;
                        if(OB_SUCCESS != (ret = encode_data(response.body, limit_size, response.body_size, sql.c_str(), log_seq, cell_info_count)))
                        {
                            TBSYS_LOG(WARN, "encode_data error, ret=%d\n", ret);
                            break;
                        }
                    }
                    response.count++;
                    cell_info_count+=(int32_t)sqls.size();
                    count--;//日志条数
                }
                //buffer已满
                if(OB_MEM_OVERFLOW == ret)
                {
                    //清空这次序列化的数据
                    response.body_size = old_pos;
                }
                else
                {
                    //设置读取到的最后一条日志号
                    response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
                    //当前mutator有SQL才设置real_seq
                    if(sqls.size() > 0)
                        response.real_seq = response.seq;
                }
            }
            //日志切换
            else if (OB_LOG_SWITCH_LOG == cmd)
            {
                if(is_big_log)
                {
                    big_log_switch_num++;
                    TBSYS_LOG(INFO, "\033[35mread log => SEQ: %lu Payload Length: %ld TYPE: %s\033[0m\n", response.seq, data_len, "BIG_LOG_SWITCH_LOG");
                }
                else
                {
                    TBSYS_LOG(INFO, "\033[35mread log => SEQ: %lu Payload Length: %ld TYPE: %s\033[0m\n", response.seq, data_len, "OB_LOG_SWITCH_LOG");
                    response.flag = LOG_SWITCH;
                    //设置读取到的最后一条日志号
                    response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
                    break;
                }

            }
            //schema相关
            else if (OB_UPS_SWITCH_SCHEMA == cmd || OB_UPS_SWITCH_SCHEMA_MUTATOR == cmd || OB_UPS_SWITCH_SCHEMA_NEXT == cmd || OB_UPS_WRITE_SCHEMA_NEXT == cmd)
            {
                //遇到schema变更，并且前面有sql直接退出；
                if( response.count > 0 ) break;
                TBSYS_LOG(INFO, "\033[35mread log => SEQ: %lu Payload Length: %ld TYPE: %s\033[0m\n", log_seq, data_len, "OB_SCHEMA");
                ret = rebuild_schema(cmd, log_seq, log_data, data_len);
                response.flag = SCHEMA_SWITCH;
                //设置读取到的最后一条日志号
                response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
                break;
            }
            else if (OB_LOG_NOP == cmd)
            {
                //todo nothing
                //设置读取到的最后一条日志号
                if(!is_big_log)
                {
                    response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
                }
            }
            else
            {
                TBSYS_LOG(ERROR, "\033[35mread log => SEQ: %lu Payload Length: %ld TYPE: %s\033[0m\n", log_seq, data_len, "UNKOWN LOG");
            }
        }
    }
    this->succ_num+=1;
    if (OB_FILE_NOT_EXIST == ret)
    {
        response.flag = LOG_END;
        ret = OB_SUCCESS;
    }
    else if (OB_READ_NOTHING == ret)
    {
        response.flag = LOG_END;
        ret = OB_SUCCESS;
    }
    else if(OB_SCHEMA_ERROR == ret)
    {
        response.flag = SCHEMA_INVAILD;
        //schema 异常，重新从capture拉取最新schema
        pull_schema(sockfd);
        ret = OB_SUCCESS;
    }
    else if(OB_MEM_OVERFLOW == ret)
    {
        response.flag = BUFFER_OVERFLOW;
        this->succ_num=0;//buf 不够用
        ret = OB_SUCCESS;
    }


    //设置读取到的最后一条日志号
    //response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
    //响应数据
    if(OB_SUCCESS == ret)
    {
        if (OB_SUCCESS != (ret = send_packet(fd, response)))
        {
            TBSYS_LOG(ERROR, "send response error, ret=%d\n", ret);
        }
        else
        {
            if(response.body != NULL && response.body_size > 0)
            {
                memset(response.body, 0, response.body_size);
            }
            if(response.count > 0)
            {
               TBSYS_LOG(INFO, "\033[33msend response success, last read seq=%lu, sno=%d, count=%d ,cell_info_count=%d, size=%ld\033[0m\n", response.seq, request.sno, response.count, cell_info_count, response.body_size);
            }
            else
            {
                TBSYS_LOG(INFO, "\033[33msend empty response success, last read seq=%lu, sno=%d, count=%d\033[0m\n", response.seq, request.sno, response.count);
            }
        }
    }
    else
    {
        response.body = const_cast<char*>("error");
        response.body_size = (int64_t)strlen(response.body);
        response.flag = AGENT_ERROR;
        response.count = 0;
        if (OB_SUCCESS != (ret = send_packet(fd, response)))
        {
            TBSYS_LOG(ERROR, "send error response error, ret=%d\n", ret);
        }
    }

    //关闭日志文件
    if (OB_SUCCESS == ret && reader.is_opened())
    {
      ret = reader.close();
      if(OB_SUCCESS != ret)
      {
          TBSYS_LOG(ERROR, "log reader close error!\n");
      }
    }

    return ret;
}


//数据格式为
int LogAgent::handler_pull_crc_request(int fd, char* log_dir, pull_request request)
{
    int ret = OB_SUCCESS;

    //2Mlog
    bool is_big_log=false;
    int  big_log_switch_num=0;
    int  big_log_num=0;

    //日志读取组件
    ObLogReader log_reader;
    ObRepeatedLogReader reader;
    ObLogCursor end_cursor;

    LogCommand cmd;
    uint64_t log_seq = 0;
    char* log_data = NULL;
    int64_t data_len = 0;

    int log_name = atoi(request.log_name);//请求的文件名
    uint64_t last_log_seq = (const uint64_t)request.seq;//请求中上一次的日志号

    //拉取的日志数
    int count = request.count;

    int64_t limit_size = DATA_BUFFER_LEN;
    //响应
    pull_response response;
    response.sno = request.sno;
    response.type = PULL_LOG_CRC_RESPONSE;
    response.version = VERSION_N;
    response.seq = last_log_seq;
    response.real_seq = 0;
    response.flag = 0;
    response.count = 0;
    response.body = sql_buf;
    response.body_size = 0;
    int32_t cell_info_count = 0;
    int64_t old_pos = response.body_size;
    //读取日志数据
    if (OB_SUCCESS != (ret = log_reader.init(&reader, log_dir, log_name, 0, false)))
    {
        TBSYS_LOG(ERROR, "ObLogReader init error[err=%d]\n", ret);
    }

    //init log reader,
    if(last_log_seq > 0)
    {
        while (OB_SUCCESS == (ret = log_reader.read_log(cmd, log_seq, log_data, data_len)))
        {
            if(log_seq>=last_log_seq)
                break;
        }
        if(log_seq!=last_log_seq)
        {
            TBSYS_LOG(ERROR, "ObLogReader init error, last_log_seq[%lu] cur_log_seq[%lu]\n",
                      last_log_seq,log_seq);
            ret=OB_INVALID_LOG;
        }
    }
    big_log_writer.reset_Last_tranID();//初始化big log,last tran id
    while (big_log_num < 1 && count > 0 && OB_SUCCESS == ret )
    {
        if(is_big_log==false && cell_info_count > SQL_BUFFER_LEN)
        {
            break;
        }

        if (OB_SUCCESS != (ret = log_reader.read_log(cmd, log_seq, log_data, data_len)) &&
            OB_FILE_NOT_EXIST != ret && OB_READ_NOTHING != ret && OB_LAST_LOG_RUINNED != ret)
        {
            TBSYS_LOG(ERROR, "ObLogReader read error[ret=%d]\n", ret);
        }
        else if (OB_LAST_LOG_RUINNED == ret)
        {
            TBSYS_LOG(ERROR, "last_log[%s] broken!\n", end_cursor.to_str());
        }
        else if (OB_FILE_NOT_EXIST == ret)
        {
            TBSYS_LOG(WARN, "log file not exist\n");
        }
        else if (OB_READ_NOTHING == ret)
        {
            //TBSYS_LOG(ERROR, "\033[31mwaiting log data\033[0m\n");
            //设置读取到的最后一条日志号
            if(!is_big_log)
            {
                response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
            }
        }
        else if (OB_SUCCESS != (ret = log_reader.get_next_cursor(end_cursor)))
        {
            TBSYS_LOG(ERROR, "log_reader.get_cursor()=>%d\n",  ret);
        }
        else
        {
            current_file_id = log_name;
            //超过2M日志，单独处理
            //普通日志
            if (OB_LOG_UPS_MUTATOR == cmd || OB_UPS_BIG_LOG_DATA == cmd)
            {
                //超过2M日志，单独处理
                if( OB_UPS_BIG_LOG_DATA == cmd )
                {
                  is_big_log=true;
                  bool is_all_completed=false;
                  if(OB_SUCCESS!=(ret=big_log_writer.package_big_log(log_data,data_len,is_all_completed)))
                  {
                    TBSYS_LOG(ERROR, "SEQ:%lu,package_big_log fail,ret=%d",log_seq,ret);
                  }
                  if(is_all_completed)
                  {
                    int32_t temp_len;
                    big_log_writer.get_buffer(log_data,temp_len);
                    data_len=(int64_t)temp_len;
                    is_big_log=false;
                    big_log_num++;
                    TBSYS_LOG(DEBUG, "package_big_log completed SEQ:%lu ,len=%lu",log_seq,data_len);
                    if(big_log_switch_num>0)
                    {
                        response.flag=BIG_LOG_SWITCH;
                        response.flag+=big_log_switch_num;
                    }
                  }
                  else
                  {
                      continue;
                  }
                }
                //TBSYS_LOG(INFO, "\033[35mread log => SEQ: %lu Payload Length: %ld TYPE: %s\033[0m\n", log_seq, data_len, "OB_LOG_UPS_MUTATOR");
                //解析sql
                vector<string> sqls;
                int64_t data_checksum_before;
                int64_t data_checksum_after;
                old_pos = response.body_size;
                if (OB_SUCCESS != (ret = rebuild_sql(log_seq, log_data, data_len, sqls)))
                {
                    TBSYS_LOG(ERROR, "rebuild_sql error, ret=%d\n", ret);
                }
                else if(sqls.size() <= 0)
                {
                    TBSYS_LOG(DEBUG, "rebuild_sql maybe error, count:%d\n", (int32_t)sqls.size());
                }
                else if(sqls.size() > 0 && OB_SUCCESS != (ret = get_checksum(log_seq, log_data, data_len, data_checksum_before,data_checksum_after)))
                {
                    TBSYS_LOG(ERROR, "get_checksum error, ret=%d\n", ret);
                }
                else if(sqls.size() > 0 && OB_SUCCESS != (ret = encode_data_header(response.body, limit_size, response.body_size,log_seq,data_checksum_before,data_checksum_after, 0)))
                {
                    TBSYS_LOG(ERROR, "encode_data_header error, ret=%d\n", ret);
                }
                else
                {
                    response.count++;
                    cell_info_count+=(int32_t)sqls.size();
                    count--;//日志条数
                }
                //buffer已满
                if(OB_MEM_OVERFLOW == ret)
                {
                    //清空这次序列化的数据
                    response.body_size = old_pos;
                }
                else
                {
                    //设置读取到的最后一条日志号
                    response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
                    //当前mutator有SQL才设置real_seq
                    if(sqls.size() > 0)
                        response.real_seq = response.seq;
                }
            }
            //日志切换
            else if (OB_LOG_SWITCH_LOG == cmd)
            {
                if(is_big_log)
                {
                    big_log_switch_num++;
                    TBSYS_LOG(INFO, "\033[35mread log => SEQ: %lu Payload Length: %ld TYPE: %s\033[0m\n", request.seq, data_len, "BIG_LOG_SWITCH_LOG");
                }
                else
                {
                    TBSYS_LOG(INFO, "\033[35mread log => SEQ: %lu Payload Length: %ld TYPE: %s\033[0m\n", request.seq, data_len, "OB_LOG_SWITCH_LOG");
                    response.flag = LOG_SWITCH;
                    //设置读取到的最后一条日志号
                    response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
                    break;
                }
            }
            //schema相关
            else if (OB_UPS_SWITCH_SCHEMA == cmd || OB_UPS_SWITCH_SCHEMA_MUTATOR == cmd || OB_UPS_SWITCH_SCHEMA_NEXT == cmd || OB_UPS_WRITE_SCHEMA_NEXT == cmd)
            {
                //遇到schema变更，并且前面有sql直接退出；
                if( response.count > 0 ) break;
                TBSYS_LOG(INFO, "\033[35mread log => SEQ: %lu Payload Length: %ld TYPE: %s\033[0m\n", log_seq, data_len, "OB_SCHEMA");
                ret = rebuild_schema(cmd, log_seq, log_data, data_len);
                response.flag = SCHEMA_SWITCH;
                //设置读取到的最后一条日志号
                response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
                break;
            }
            else if (OB_LOG_NOP == cmd)
            {
                //todo nothing
                //设置读取到的最后一条日志号
                if(!is_big_log)
                {
                    response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
                }
            }
            else
            {
                TBSYS_LOG(ERROR, "\033[35mread log => SEQ: %lu Payload Length: %ld TYPE: %s\033[0m\n", log_seq, data_len, "UNKOWN LOG");
            }
        }
    }

    if (OB_FILE_NOT_EXIST == ret)
    {
        response.flag = LOG_END;
        ret = OB_SUCCESS;
    }
    else if (OB_READ_NOTHING == ret)
    {
        response.flag = LOG_END;
        ret = OB_SUCCESS;
    }
    else if(OB_SCHEMA_ERROR == ret)
    {
        response.flag = SCHEMA_INVAILD;
        //schema 异常，重新从capture拉取最新schema
        pull_schema(sockfd);
        ret = OB_SUCCESS;
    }
    else if(OB_MEM_OVERFLOW == ret)
    {
        response.flag = BUFFER_OVERFLOW;
        ret = OB_SUCCESS;
    }


    //设置读取到的最后一条日志号
    //response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
    //响应数据
    if(OB_SUCCESS == ret)
    {
        if (OB_SUCCESS != (ret = send_packet(fd, response)))
        {
            TBSYS_LOG(ERROR, "send response error, ret=%d\n", ret);
        }
        else
        {
            if(response.body != NULL && response.body_size > 0)
            {
                memset(response.body, 0, response.body_size);
            }
            if(response.count > 0)
            {
               TBSYS_LOG(INFO, "\033[33msend response success, last read seq=%lu, sno=%d, count=%d ,cell_info_count=%d, size=%ld\033[0m\n", request.seq, request.sno, response.count, cell_info_count, response.body_size);
            }
            else
            {
                TBSYS_LOG(INFO, "\033[33msend empty response success, last read seq=%lu, sno=%d, count=%d\033[0m\n", request.seq, request.sno, response.count);
            }
        }
    }
    else
    {
        response.body = const_cast<char*>("error");
        response.body_size = (int64_t)strlen(response.body);
        response.flag = AGENT_ERROR;
        response.count = 0;
        if (OB_SUCCESS != (ret = send_packet(fd, response)))
        {
            TBSYS_LOG(ERROR, "send error response error, ret=%d\n", ret);
        }
    }

    //关闭日志文件
    if (OB_SUCCESS == ret && reader.is_opened())
    {
      ret = reader.close();
      if(OB_SUCCESS != ret)
      {
          TBSYS_LOG(ERROR, "log reader close error!\n");
      }
    }

    return ret;
}


int LogAgent::parse_request(char* buf, int64_t data_len, pull_request &request)
{
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    int32_t length = 0;//数据长度
    ret = serialization::decode_i32(buf, data_len, pos, &length);
    if (ret != OB_SUCCESS || data_len - 4 != length)
    {
       TBSYS_LOG(INFO, "buffer not compelete, data_len=%ld , length=%d!\n" ,data_len ,length);
    }
    else if (OB_SUCCESS != (ret = serialization::decode_i32(buf, data_len, pos, &request.sno)))
    {
       TBSYS_LOG(INFO, "deserialize sno error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
    }
    else if (OB_SUCCESS != (ret = serialization::decode_i32(buf, data_len, pos, &request.type)))
    {
       TBSYS_LOG(INFO, "deserialize type error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
    }
    else
    {
        int32_t log_name_len;
        if (OB_SUCCESS == (ret = serialization::decode_i32(buf, data_len, pos, &log_name_len)))
        {
            request.log_name = new char[log_name_len + 1];
            memcpy(request.log_name, buf + pos, log_name_len);
            request.log_name[log_name_len] = '\0';
            pos += log_name_len;


            if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &request.seq)))
            {
               TBSYS_LOG(INFO, "deserialize seq error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
            }
            else if (OB_SUCCESS != (ret = serialization::decode_i32(buf, data_len, pos, &request.version)))
            {
               TBSYS_LOG(INFO, "deserialize version error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
            }
            else if (OB_SUCCESS != (ret = serialization::decode_i32(buf, data_len, pos, &request.count)))
            {
               TBSYS_LOG(INFO, "deserialize count error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
            }
            else
            {
                int32_t extend_len;
                if (OB_SUCCESS != (ret = serialization::decode_i32(buf, data_len, pos, &extend_len)))
                {
                   TBSYS_LOG(INFO, "deserialize extend_len error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
                }
                else
                {
                    request.extend = new char[extend_len];
                    request.extend_size = extend_len;
                    memcpy(request.extend, buf + pos, extend_len);
                    pos += extend_len;
                }

                //fprintf(stdout, "sno=%d, type=%d, log_name=%s, seq=%d, version=%d, count=%d, extend_len=%d, ret=%d (pos=%ld,length=%d,data_len=%ld)\n" ,request.sno, request.type, request.log_name, request.seq, request.version, request.count, extend_len, ret, pos, length, data_len);
            }
        }
    }
    return ret;

}

ssize_t LogAgent::readn(int fd, void *vptr, size_t n)
{
    size_t nleft;
    ssize_t nread;
    char *ptr;
    ptr = (char*)vptr;
    nleft = n;
    while (nleft > 0)
    {
        if((nread = read(fd, ptr, nleft)) < 0)
        {
            if(errno == EINTR)
                nread = 0;
            else
                return -1;
        }
        else if(nread == 0)
        {
            break;
        }
        nleft -= nread;
        ptr += nread;
    }
    return n - nleft;
}

ssize_t LogAgent::writen(int fd,const void *vptr, size_t n)
{
    size_t nleft;
    ssize_t nwrite;
    const char *ptr;
    ptr = (const char*)vptr;
    nleft = n;
    while (nleft > 0)
    {
        if((nwrite = write(fd, ptr, nleft)) <= 0)
        {
            if(nwrite < 0 && errno == EINTR)
                nwrite = 0;
            else
                return -1;
        }

        nleft -= nwrite;
        ptr += nwrite;
    }
    return n;
}

/**
 * @brief 读取完整的一个数据包
 * @param fd
 * @param vptr
 * @param n
 * @return
 */
int LogAgent::receive_packet(int fd, pull_request &request)
{
    char* buf = new char[READ_BUFFER_LEN];
    int32_t length = 0;//数据长度
    int64_t length_data_len = 4;//前4个字节表示包的长度
    int64_t pos = 0;
    if((int64_t)readn(fd, buf, (size_t)length_data_len) != length_data_len)//先读取前4个字节
    {
        TBSYS_LOG(ERROR, "readn length error!\n");
        return -1;
    }
    else if(OB_SUCCESS != serialization::decode_i32(buf, length_data_len, pos, &length))//解析长度
    {
        TBSYS_LOG(ERROR, "decode packet length error!\n");
        return -1;
    }
    else if((int32_t)readn(fd, buf + length_data_len, (size_t)length) != length)//读取真实的包数据
    {
        TBSYS_LOG(ERROR, "readn data error!\n");
        return -1;
    }
    else if (OB_SUCCESS != parse_request(buf, length + length_data_len, request))//解析数据包为request结构体
    {
        TBSYS_LOG(ERROR, "parse_request error!\n");
        return -1;
    }
    delete[] buf;
    return OB_SUCCESS;
}

int LogAgent::send_packet(int fd, const pull_response response)
{
    cond_.lock();//防止并发发送数据
    int ret = OB_SUCCESS;
    if(connected_flag == CONNECTED)
    {
        //char* send_buf = new char[WRITE_BUFFER_LEN];
        int64_t size = 0;

        if(OB_SUCCESS != (ret = build_response(socket_send_buf, response, size)))
        {
            TBSYS_LOG(ERROR, "build_response error!\n");
        }
        else if((int32_t)writen(fd, socket_send_buf, size) != size)
        {
            TBSYS_LOG(ERROR, "send packet error!\n");
            ret = DISCONNECTED;
        }
        //delete[] send_buf;
        memset(socket_send_buf, 0, size);
    }
    else
    {
        ret = DISCONNECTED;
    }
    cond_.unlock();
    return ret;
}

int LogAgent::send_schema(int fd, uint64_t seq, CommonSchemaManagerWrapper &schema)
{
    int ret = OB_SUCCESS;

    //确保大小计算正确
    schema.get_impl_ref().set_serialize_whole();
    pull_response response;
    response.sno = 0;
    response.type = SCHEMA_REPORT;
    response.seq = seq;
    response.real_seq = 0;
    response.version = VERSION_N;
    response.count = 0;
    response.flag = current_file_id;//设置为请求的文件名
    response.body = new char[schema.get_serialize_size()];
    response.body_size = 0;

    int64_t buffer_len = schema.get_serialize_size();

    //schema.get_impl_ref().set_serialize_whole();
    if(schema.schema_mgr_impl_ == NULL)
    {
        TBSYS_LOG(INFO, "NULL ret=%d\n",ret);
    }
    if(OB_SUCCESS != (ret = schema.serialize(response.body, buffer_len, response.body_size)))
    {
       TBSYS_LOG(INFO, "schema serialize error ret=%d\n", ret);
    }
    else if(OB_SUCCESS != (ret = send_packet(fd, response)))
    {
       TBSYS_LOG(INFO, "\033[31msend schema error...\033[0m\n");
    }
    else
    {
       TBSYS_LOG(INFO, "\033[33msend schema success, size:%ldbyte current_file_id=%d\033[0m\n", response.body_size, current_file_id);
        delete[] response.body;
    }
    return ret;
}


//################################华丽的分割线 3#########################################

/**
 * @brief 信号量的处理
 * @param sig
 */
void handle(const int sig)
{
    switch(sig)
    {
      case SIGPIPE:
       TBSYS_LOG(INFO, "receive signal sig=%d, ignore this!\n", sig);
        break;
      default:
       TBSYS_LOG(INFO, "receive signal sig=%d, please check!\n", sig);
        exit(0);
    }

}

void LogAgent::print_version()
{
    fprintf(stderr, "BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
}

void LogAgent::print_usage(const char *prog_name)
{
    fprintf(stderr, "%s\n"
            "\t-b|--sql buffer MB\n"
            "\t-d|--ups commit log dir\n"
            "\t-m|--master ip\n"
            "\t-p|--master port\n"
            "\t-v|--version\n"
            "\t-h|--help\n",prog_name);
}

void LogAgent::parse_cmd_line(const int argc,  char *const argv[])
{
    int opt = 0;
    const char* opt_string = "b:d:m:p:vh";
    struct option longopts[] =
      {
        {"buffer", 1, NULL, 'b'},
        {"dir", 1, NULL, 'd'},
        {"master", 1, NULL, 'm'},
        {"port", 1, NULL, 'p'},
        {"version", 1, NULL, 'v'},
        {"help", 1, NULL, 'h'},
        {0, 0, 0, 0}
      };
    bool set_ip = false;
    bool set_port = false;
    bool set_dir = false;
    while((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
      switch (opt) {
        case 'b':
          DEFAULT_BUFFER_LEN = 1024*1024*static_cast<int64_t>(strtol(optarg, NULL, 0));
          this->balanceBuffer();
          break;
        case 'm':
          snprintf(master_ip, OB_IP_STR_BUFF, "%s", optarg);
          set_ip = true;
          break;
        case 'p':
          master_port = static_cast<int32_t>(strtol(optarg, NULL, 0));
          set_port = true;
          break;
        case 'd':
          snprintf(ups_log_dir, sizeof (ups_log_dir), "%s", optarg);
          set_dir = true;
          break;
        case 'v':
          print_version();
          exit(1);
        case 'h':
          print_usage(basename(argv[0]));
          exit(1);
        default:
          break;
      }
    }

    if(!set_ip || !set_port || !set_dir)
    {
         print_usage(basename(argv[0]));
         exit(1);
    }

}
void checkCmd(const int argc,  char *const argv[])
{
    int fd =open("logs", 0);
    if (fd != -1) close(fd);
    else system("mkdir logs");
    if(argc==2)
    {
        string op=argv[1];
        if(op=="-v"||op=="-V")
        {
            //printf("BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
            printf("CDCAgent SVN_VERSION: %s\n", svn_version());
            printf("BUILD_TIME: %s %s\n", build_date(), build_time());
            printf("BUILD_FLAGS: %s\n\n", build_flags());
        }
        else
        {
            printf("%s\n"
                    "\t-b|--sql buffer MB\n"
                    "\t-d|--ups commit log dir\n"
                    "\t-m|--master ip\n"
                    "\t-p|--master port\n"
                    "\t-v|--version\n"
                    "\t-h|--help\n",argv[0]);
        }
        exit(1);
    }
}

/**
 * @brief 主函数
 * @param argc
 * @param argv
 * @return
 */
int main(int argc, char* argv[])
{
    checkCmd(argc, argv);
    signal(SIGABRT, handle);
    signal(SIGTERM, handle);
    signal(SIGINT, handle);
    signal(SIGPIPE, handle);
    signal(SIGHUP, handle);

    int ret = 0;

    TBSYS_LOGGER.setLogLevel("INFO");
    TBSYS_LOGGER.setFileName("logs/agent.log");
    TBSYS_LOGGER.setMaxFileSize(MAX_LOG_SIZE);
    //初始化内存池
    ob_init_memory_pool();
    //===============守护线程==================

    if (getppid() == 1) {
        return 0;
    }
    int pid = fork();
    if (pid < 0) exit(1);
    if (pid > 0) return pid;

    int fd =open("/dev/null", 0);
    if (fd != -1) {
        dup2(fd, 0);
        close(fd);
    }

    LogAgent agent;
    agent.start(argc, argv);

    //pthread_exit(NULL);//等待进程结束

    return ret;
}
