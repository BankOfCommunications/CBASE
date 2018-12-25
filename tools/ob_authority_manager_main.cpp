/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_authority_manager_main.cpp is for log on authority managemnet tool
 *
 * Version: $Id: ob_authority_manager_main.cpp,v 0.1 08/31/2011 04:58:16 PM yangke Exp $
 *
 * Authors:
 *   Yang Ke <yangke.pt@taobao.com>
 *     - 
 *   
 */
#include <unistd.h>
#include <cstdio>
#include <stdlib.h>
#include "ob_authority_manager.h"

using namespace oceanbase::common;

void print_usage()
{
	fprintf(stderr, "\nUsage: authority_management <-u rootname> <-h ip> <-p port>\n"
			"-u	user name who must has root authority\n"
			"-h	root server ip address\n"
			"-p	root server port\n");
}

int main(int argc, char* argv[])
{
	char *user_name = NULL;
	char *ip_address = NULL;
	int64_t port = -1;
	int oc = -1;
	int ret = 0;
	while(0 == ret && 
			(oc = getopt(argc, argv, "u:h:p:")) != -1)
	{
		switch(oc)
		{
		case 'u':
			user_name = optarg;
			break;
		case 'h':
			ip_address = optarg;
			break;
		case 'p':
			port = atoi(optarg);
			if (port == 0)
			{
				print_usage();
				ret = -1;
			}
			break;
		default:
			print_usage();
			ret = -1;	
		}
	}

	if (0 != ret || NULL == user_name ||
			ip_address == NULL || port == -1)
	{
		ret = -1;
		print_usage();
	}

	if (0 == ret)
	{
		ob_init_memory_pool();
		AuthorityManager author_manager;
		author_manager.set_user_name(user_name);
		author_manager.set_ip_address(ip_address);
		author_manager.set_port(port);
		author_manager.log_on();
	}

	return ret;
}




