/*
 * =====================================================================================
 *
 *       Filename:  merge_main.cpp
 *
 *    Description:  W
 *
 *        Version:  1.0
 *        Created:  2014年10月13日 19时11分26秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  liyongfeng (ECNU), <lee091201@gmail.com><liyf.sdc@sdc.com>
 *   Organization:  DaSE&Bank of Communication
 *
 * =====================================================================================
 */
#include "common/ob_client_manager.h"
#include "common/ob_base_client.h"
#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/serialization.h"
#include "common/ob_result.h"
#include "tbsys.h"

using namespace oceanbase::common;

int do_rs_merge_stat(ObBaseClient &client)
{
	int ret = OB_SUCCESS;
	printf("do_rs_merge_stat...\n");
	static const int32_t MY_VERSION = 1;
	char *buff = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
	ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);
	if(NULL == buff) {
		printf("no memory\n");
	}
	else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), 32))) {
		printf("failed to serialize, err=%d\n", ret);
	}
	else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_STAT, MY_VERSION, 20000000, msgbuf))) {
		printf("failed to send request, err=%d\n", ret);
	} else {
		ObResultCode result_code;
		msgbuf.get_position() = 0;
		if(OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
			printf("failed to deserialize response, err=%d\n", ret);
		}
		else if(OB_SUCCESS != (ret = result_code.result_code_)) {
			printf("failed to do_rs_admin, err=%d\n", result_code.result_code_);
		} else {
			printf("%s\n", msgbuf.get_data() + msgbuf.get_position());
			if(0 == strcmp("merge: DONE", msgbuf.get_data() + msgbuf.get_position())) {
				printf("YES!");
			} else {
				printf("NO!");
			}
		}
	}
	if(NULL != buff) {
		delete [] buff;
		buff = NULL;
	}

	return ret;
}

int main(/*int argc, char *argv[]*/)
{
	int ret = OB_SUCCESS;
	ob_init_memory_pool();
	TBSYS_LOGGER.setFileName("/home/admin/lyf/do_rs_merge.log");
	TBSYS_LOGGER.setLogLevel("INFO");
	ObServer server(ObServer::IPV4, "182.119.174.56", 2500);
	ObBaseClient client;
	if(OB_SUCCESS != (ret = client.initialize(server))) {
		printf("failed to init client, err=%d\n", ret);
	} else {
		fprintf(stderr,"server[%s]\n", server.to_cstring());
		ret = do_rs_merge_stat(client);
	}

	return ret == OB_SUCCESS ? 0 : -1;
}
