/*
 * =====================================================================================
 *
 *       Filename:  db_queue.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2014年10月20日 14时56分47秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  liyongfeng (ECNU), <lee091201@gmail.com><liyf.sdc@sdc.com>
 *   Organization:  DaSE&Bank of Communication
 *
 * =====================================================================================
 */

#include "db_queue.h"

bool g_cluster_state = true;

/*
//add by liyongfeng, 20141014, monitor the state of daily merge
template <class T>
void ComsumerQueue<T>::monitor(long id)
{
	int ret = 0;
	int32_t state = 0;//记录每次获取到merge状态

	TBSYS_LOG(INFO, "in monitor thread, id=%ld", id);
	while (running_) {
		if (atomic_read(&consumer_quiting_nr_) == nr_comsumer_) {
			TBSYS_LOG(INFO, "all consumer quit, quit monitor id=%ld", id);
			break;
		} else {
			//send request to get the state of daily merge
			RPC_WITH_RETRIES_SLEEP(db_->get_daily_merge_state(state), 3, ret);//重试3次,每次间隔5s
			if(ret != 0) {
				//send request failed
				TBSYS_LOG(ERROR, "failed to send request to rootserver, to get the state of daily merge, err=%d", ret);
				//将is_merge_置为-1,禁止ob_import导入数据
				if (1 == atomic_read(&is_merge_)) {
					atomic_sub(2, &is_merge_);
				} else if (0 == atomic_read(&is_merge_)) {
					atomic_sub(1, &is_merge_);
				} else {
					//TBSYS_LOG(INFO, "forbid producer running, is_merge_=%d", atomic_read(&is_merge_));
				}
				TBSYS_LOG(WARN, "forbid producer running, is_merge_=%d", atomic_read(&is_merge_));
				TBSYS_LOG(INFO, "quit monitor id=%ld", id);
				//修改全局状态,通知主线程异常状态,程序会异常退出
				g_root_server_state = false;
				break;
			} else {
				//首先判断上一次是什么状态,然后根据获取到当前状态,修改is_merge_
				if(1 == atomic_read(&is_merge_)) {//上一次是merge: DONE
					if (1 == state) {
						TBSYS_LOG(DEBUG, "current state [merge: DONE]");
					} else if (0 == state) {
						TBSYS_LOG(DEBUG, "current state [merge: DOING]");
						atomic_sub(1, &is_merge_);
					} else if (-1 == state) {
						TBSYS_LOG(DEBUG, "can't get merge state");
						atomic_sub(2, &is_merge_);
					} else {
						TBSYS_LOG(ERROR, "invalid merge state, state=%d", state);
						atomic_sub(2, &is_merge_);
					}
				} else if (0 == atomic_read(&is_merge_)) {//上一次是merge: DOING or merge: TIMEOUT
					if (1 == state) {
						TBSYS_LOG(DEBUG, "current state [merge: DONE]");
						atomic_add(1, &is_merge_);
					} else if (0 == state) {
						TBSYS_LOG(DEBUG, "current state [merge: DOING]");
					} else if (-1 == state) {
						TBSYS_LOG(DEBUG, "can't get merge state");
						atomic_sub(1, &is_merge_);
					} else {
						TBSYS_LOG(ERROR, "invalid merge state, state=%d", state);
						atomic_sub(1, &is_merge_);
					}
				} else {//上一次获取merge状态失败,ob_import过程已经被禁止,正在准备退出,不管本次获取到状态
					TBSYS_LOG(WARN, "forbid producer running, is_merge_=%d", atomic_read(&is_merge_));
					//修改全局状态,通知主线程异常状态,程序会异常退出
					TBSYS_LOG(INFO, "quit monitor id=%ld", id);
					//g_root_server_state = false;
					break;
				}
			}

			sleep(30);//每次间隔30s获取一次每日合并状态
		}
	}
}
//add:end
*/
