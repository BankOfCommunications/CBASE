/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_authority_manager.h is for 
 *
 * Version: $Id: ob_authority_manager.h,v 0.1 08/31/2011 05:00:33 PM yangke Exp $
 *
 * Authors:
 *   Yang Ke <yangke.pt@taobao.com>
 *     - some work details if you want
 *   
 */
#ifndef __OCEANBASE_COMMON_OB_AUTHORITY_MANAGER_H__
#define __OCEANBASE_COMMON_OB_AUTHORITY_MANAGER_H__

#include <iostream>
#include <string>
#include "ob_malloc.h"
#include "client/cpp/oceanbase.h"
#include "ob_vector.h"

namespace oceanbase
{
  namespace common
	{
		class AuthorityManager
		{
		public:
			AuthorityManager():port_(-1) , ob_(NULL)
			{
			}
			~AuthorityManager()
			{
				if (NULL != ob_)
				{
					ob_api_destroy(ob_);

				}
			}
			void log_on();
			void print_privilege(const char* print_user_name, const char* table_name);
			void set_user_name(const char *user_name);
			void set_port(int64_t port);
			void set_ip_address(const char *ip_address);
			void print_help();
		private:
			bool verify();
			int get_ch_without_echo(void);
			OB_ERR_CODE get_ob_connection(char* passwd);
			void wait_for_input();
			char* trim(char *str);
			void split_by_space(const char *str, ObVector<char*> &list);
			int find_command_index(const char *command);
			OB_ERR_CODE excute_command(int command_index, ObVector<char*> &list);
		private:
			char ip_address_[OB_IP_STR_BUFF];
			int64_t port_;
			char user_name_[OB_MAX_USERNAME_LENGTH];
			OB *ob_;
			typedef enum option_type_st
			{
				CREATE_USER,
				DELETE_USER,
				CHANGE_PASSWD,
				SHOW_USER,
				UPDATE_PRIVILEGE,
				SHOW_PRIVILEGE,
				HELP,
				QUIT,
			} OPTION_TYPE;
		};
	}
}

#endif //__OCEANBASE_COMMON_OB_AUTHORITY_MANAGER_H__

