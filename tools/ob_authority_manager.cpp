/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_authority_manager.cpp is for 
 *
 * Version: $Id: ob_authority_manager.cpp,v 0.1 08/31/2011 05:01:20 PM yangke Exp $
 *
 * Authors:
 *   Yang Ke <yangke.pt@taobao.com>
 *     - some work details if you want
 *   
 */
#include "ob_authority_manager.h"
#include <iostream>
#include <stdio.h>
#include <termios.h>
#include <unistd.h>
#include <strings.h>
#include <iostream>
#include <readline/readline.h>
#include <readline/history.h>
#include <strings.h>
#include <string.h>
#include "ob_authority_manager.h"
#include "stringutil.h"
#include "ob_vector.h"
#include "ob_define.h"

using namespace std;
using namespace tbsys;
using namespace oceanbase::common;

namespace oceanbase
{
	namespace common
	{
		const char *COMMANDS[] = {"create_user", "delete_user",
			"change_password", "show_user", "update_privilege",
			"show_privilege", "help", "quit"};
		const int32_t COMMANDS_SIZE = 8;
		const int32_t COMMAND_STRING_MAX_LENGTH = 400;
		const int32_t COMMAND_PART_MAX_LENGTH = 100;
	}
}

void AuthorityManager::print_help()
{
	fprintf(stderr, "\nUsage: \n"
			"create_user		user_name	password  		--  create user with name:user_name, and password: password\n"
			"delete_user		user_name            			--  delete user: user-name\n"
			"change_password		user_name	password 		--  change the password\n"
			"show_user							--  show all users\n"
			"update_privilege	[add|del] [r|w] table_name user_name	--  update user:user_name privilege on table:table_name\n"
			"show_privilege		[user_name]          			--  if no user_name, show all users's privilege, otherwise show user_name's privilege\n"
			"help							   	--  show help info\n"
			"quit							   	--  quit the tool\n");
}

void AuthorityManager::log_on()
{
	int ret = 0;
	if (!verify())
	{
		cerr << "Log on failed!" << endl;
		ret = -1;
	}

	if (0 == ret)
	{
		wait_for_input();
	}
}

void AuthorityManager::wait_for_input()
{

	char *input, *line;
	OB_ERR_CODE ret = OB_ERR_SUCCESS;
	while(true)
	{
		line = readline("authority_management> ");
		if (NULL == line)
		{
			continue;
		}
		input = trim(line);
		if (*input)
		{
			add_history(input);
		}
		free(line);

		// according to input, process
		input = CStringUtil::strToLower(input);
		ObVector<char*> command_parts;
		split_by_space(input, command_parts);
		ob_free(input);
		if (command_parts.size() < 1)
		{
			cerr << "command not found!" << endl;
			print_help();
			continue;
		}

		int command_index = find_command_index(command_parts[0]);			
		if ( -1 == command_index)
		{
			cerr << "command not found!" << endl;
			print_help();
			continue;
		}

		// 删除其中包含的元素
		ret = excute_command(command_index, command_parts);
		for (int i = 0; i < command_parts.size(); i++)
		{
			ob_free(command_parts[i]);
		}

		if (OB_ERR_QUIT == ret)
		{
			cout << "quit!" << endl;
			break;
		}
		else if (OB_ERR_SUCCESS == ret)
		{
			cout << "excute  successfully" << endl;
		}
	}
}

OB_ERR_CODE AuthorityManager::excute_command(int command_index, ObVector<char *> &list)
{
	OB_ERR_CODE ret = OB_ERR_SUCCESS;
	switch(command_index)
	{
	case CREATE_USER:
		{
			if (list.size() != 3)
			{
				cerr << "command not found!" << endl;
				cerr << "create_user  user_name	  password" << endl;  
				ret = OB_ERR_COMMAND_ERROR;
			}
			if ( OB_ERR_SUCCESS == ret)
			{
				ret = ob_create_user(ob_, list[1], list[2]);
				if (OB_ERR_USER_ALREADY_EXIST == ret)
				{
					cout << "user: " << list[1] << " already exsit!" << endl;
				}
			}
			break;
		}
	case DELETE_USER:
		{
			if (list.size() != 2)
			{
				cerr << "command not found!" << endl;
				cerr << "delete_user  user_name" << endl;  
				ret = OB_ERR_COMMAND_ERROR;
			}
			if (OB_ERR_SUCCESS == ret)
			{
				ret = ob_delete_user(ob_, list[1]);
			}
			if (OB_ERR_USER_NOT_EXIST == ret)
			{
				cerr << "user: " << list[1] << " may be not exist" << endl;
			}
			break;
		}
	case CHANGE_PASSWD:
		{
			if (list.size() != 3)
			{
				cerr << "command not found!" << endl;
				cerr << "change_password  user_name   password" << endl;  
				ret = OB_ERR_COMMAND_ERROR;
			}
			if (OB_ERR_SUCCESS == ret)
			{
				ret = ob_change_passwd(ob_, list[1], list[2]);
			}

			if (OB_ERR_USER_NOT_EXIST == ret)
			{
				cerr << "user: " << list[1] << " may be not exist" << endl;
			}

			return ret;
		}
	case SHOW_USER:
		{
			OB_USER_LIST user_list;
			ret = ob_get_all_user(ob_, &user_list);
			if (ret == OB_ERR_SUCCESS)
			{
				cout << "----USERS----" << endl;
				OB_USER *cur = user_list->next;
				while (cur != NULL)
				{
					cout << cur->user_name << endl;
					cur = cur->next;
				}
				cout << "----USERS----" << endl;
			}
			if (ret == OB_ERR_SUCCESS)
			{
				ret = ob_free_user_list(user_list);
			}
			return ret;
		}
	case UPDATE_PRIVILEGE:
		{
			if (list.size() != 5)
			{
				cerr << "command not found!" << endl;
				cerr << "update_privilege [add|del] [r|w] table_name user_name" << endl;  
				ret = OB_ERR_COMMAND_ERROR;
			}

			OB_PRIVILEGE_TYPE privilege_type;
			if (OB_ERR_SUCCESS == ret)
			{
				if (strcmp(list[2], "w") == 0)
				{
					privilege_type = OB_PRIVILEGE_WRITE;
				}
				else if (strcmp(list[2], "r") == 0)
				{
					privilege_type = OB_PRIVILEGE_READ;
				}
				else if (strcmp(list[2], "x") == 0)
				{
					privilege_type = OB_PRIVILEGE_EXCUTE;
				}
				else{
					cerr << "command not found!" << endl;
					cerr << "update_privilege [add|del] [r|w] table_name user_name" << endl;  
					ret = OB_ERR_COMMAND_ERROR;
				}
			}

			if (OB_ERR_SUCCESS == ret)
			{
				if (strcmp(list[1], "del") == 0)
				{
					ret = ob_del_privilege(ob_, list[4], list[3], privilege_type);
				}
				else if (strcmp(list[1], "add") == 0)
				{
					ret = ob_add_privilege(ob_, list[4], list[3], privilege_type);
				}
				else
				{
					cerr << "command not found!" << endl;
					cerr << "update_privilege [add|del] [r|w] table_name user_name" << endl;  
					ret = OB_ERR_COMMAND_ERROR;
				}
			}
			break;
		}
	case SHOW_PRIVILEGE:
		{
			OB_PRIVILEGE_LIST  privilege_list;
			if (list.size() == 1)
			{
				ret = ob_get_all_user_privilege(ob_, &privilege_list);
			}
			else if (list.size() == 2){
				ret = ob_get_privilege(ob_, list[1], &privilege_list);
			}
			else
			{
				cerr << "command not found!" << endl;
				cerr << "show_privilege [user_name]" << endl;  
				ret = OB_ERR_COMMAND_ERROR;
			}

			if (OB_ERR_SUCCESS == ret)
			{
				OB_PRIVILEGE *cur = privilege_list->next;
				cout << "USER\t\t" << "TABLE\t\t" << "PRIVILEGE" << endl; 
				while (cur != NULL)
				{
					cout << cur->user_name << "\t\t";
					cout << cur->table_name << "\t\t";
					OB_INT privilege = cur->privilege_type;
					if ((privilege & OB_PRIVILEGE_EXCUTE) == OB_PRIVILEGE_EXCUTE)
					{
						cout << "X ";
					}
					if ((privilege & OB_PRIVILEGE_READ) == OB_PRIVILEGE_READ)
					{
						cout << "R ";
					}
					if ((privilege & OB_PRIVILEGE_WRITE) == OB_PRIVILEGE_WRITE)
					{
						cout << "W ";
					}

					cout << endl;
					cur = cur->next;
				}
			}
			break;
		}
	case HELP:
		{
			print_help();
		}
	case QUIT:
		{
			ret = OB_ERR_QUIT; 
		}
	}	
	return ret;
}

char* AuthorityManager::trim(char *string)
{
	char* result = reinterpret_cast<char *> (ob_malloc(sizeof(char) * COMMAND_STRING_MAX_LENGTH));
	if (NULL != result)
	{
		if (NULL == string)
		{
			result[0] = '\0';
		}
		else
		{
			register char *s, *t;
			for (s = string; *s == ' '; s++)
				;

			if (0 == *s)
			{
				result[0] = '\0';
			}
			else
			{
				t = s + strlen (s) - 1;
				while (t > s && *t == ' ')
					t--;
				*++t = '\0';
				strncpy(result, s, COMMAND_STRING_MAX_LENGTH);
			}
		}
	}
	return result;
}

void AuthorityManager::split_by_space(const char *str, ObVector<char*> &list)
{
	if (str!= NULL)
	{
		int64_t first_no_blank_index = -1;
		int64_t current_index = -1;
		string input = string(str);
		int64_t length = strlen(str);
		while(current_index < length)
		{
			current_index++;

			if ((*(str + current_index) != ' ') && 
					(first_no_blank_index == -1))
			{
				first_no_blank_index = current_index;
				continue;
			}

			if (((*(str + current_index) == ' ') || 
						(*(str + current_index) == '\0'))
					&& (first_no_blank_index != -1))
			{
				string part_string = input.substr(first_no_blank_index, current_index - first_no_blank_index);
				char *part_chars = reinterpret_cast<char *> (ob_malloc(sizeof(char) * COMMAND_PART_MAX_LENGTH));
				if (NULL == part_chars)
				{
					TBSYS_LOG(ERROR, "can't allocate memory for password ");
					break;
				}
				strncpy(part_chars, part_string.c_str(), COMMAND_PART_MAX_LENGTH);
				list.push_back(part_chars);
				first_no_blank_index = -1;
			}
		}
	}

	return;
}

int AuthorityManager::find_command_index(const char* command)
{
	int ret = -1;
	for(int i = 0; i < COMMANDS_SIZE; i++)
	{
		if (strcmp(command, COMMANDS[i]) == 0)
		{
			ret = i;
			break;
		}
	}

	return ret;
}

bool AuthorityManager::verify()
{
	bool result = true;
	char *password =  reinterpret_cast<char *> (ob_malloc(sizeof(char) * OB_MAX_PASSWORD_LENGTH));
	if (password == NULL)
	{
		TBSYS_LOG(ERROR, "can't allocate memory for password ");
		result = false;
	}

	if (result)
	{
		memset(password, 0, OB_MAX_PASSWORD_LENGTH);

		int cur_index = 0;
		cout << "Password: " << endl;
		char c = '\0';
		while (((c = get_ch_without_echo()) != '\n') &&
				(cur_index < OB_MAX_PASSWORD_LENGTH - 1) )
		{
			password[cur_index] = c;
			cur_index++;
		}
		password[cur_index] = '\0';

		OB_ERR_CODE ret = OB_ERR_SUCCESS;
		ret = get_ob_connection(password);
		ob_free(password);

		if (OB_ERR_SUCCESS != ret)
		{
			result = false;
		}
		else 
		{
			result = true;
		}
	}

	return result;
}

OB_ERR_CODE AuthorityManager::get_ob_connection(char* passwd)
{
	OB_ERR_CODE ret = OB_ERR_SUCCESS;
	if (NULL == ob_)
	{
		ob_ = ob_api_init();
		// TODO: 现在密码还不能使用
		ret = ob_connect(ob_, ip_address_, port_, NULL, NULL);
		if (OB_ERR_SUCCESS != ret)
		{
			cout << "Can't get connection with: " << ip_address_;
			cout << ":";
			cout << port_ << endl;
		}
	}
	return ret;
}

int AuthorityManager::get_ch_without_echo()
{
	struct termios old_termino, new_termino;
	int ch; 

	// 保存原始数据用于恢复
	tcgetattr(STDIN_FILENO, &old_termino);
	new_termino = old_termino;
	new_termino.c_lflag &= ~( ICANON | ECHO );
	tcsetattr(STDIN_FILENO, TCSANOW, &new_termino); 
	ch = getchar();
	tcsetattr(STDIN_FILENO, TCSANOW, &old_termino );
	return ch; 
}

void AuthorityManager::set_ip_address(const char *ip_address)
{
	strncpy(ip_address_, ip_address, OB_IP_STR_BUFF);
}

void AuthorityManager::set_port(int64_t port)
{
	port_ = port;
}

void AuthorityManager::set_user_name(const char *user_name)
{
	strncpy(user_name_, user_name, OB_MAX_USERNAME_LENGTH);
}

