// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <iostream>
#include <string>
using namespace std;

#include "common/config.h"
#include "include/ceph_features.h"

#include "mon/MonMap.h"
#include "mon/Monitor.h"
#include "mon/MonitorDBStore.h"
#include "mon/MonClient.h"

#include "msg/Messenger.h"
#include "include/CompatSet.h"

#include "common/ceph_argparse.h"
#include "common/pick_address.h"
#include "common/Timer.h"
#include "common/errno.h"
#include "common/Preforker.h"

#include "global/global_init.h"
#include "global/signal_handler.h"
#include "perfglue/heap_profiler.h"
#include "include/assert.h"
#include "auth/AuthSessionHandler.h"

#define dout_subsys ceph_subsys_mon

int read_message(Message **pm, AuthSessionHandler *session_security_copy)
{
	return 0;
}

int write_message(const ceph_msg_header& h, const ceph_msg_footer& f, bufferlist& body)
{
	return 0;
}

Message *fetch_msg(Message *m, int accept_sd)
{
	printf("msg\n");
	return m;
}

int tcp_read(char *buf, unsigned len)
{
	return 0;
}

int tcp_write(const char *buf, unsigned len)
{
	return 0;
}

int set_socket_options(int sd)
{
	(void)sd;
	return 0;
}

int accept(int accept_sd)
{
	int ret;
	// vars
	bufferlist addrs;
	entity_addr_t socket_addr;
	socklen_t len;
	int r;
	char banner[strlen(CEPH_BANNER)+1];
	bufferlist addrbl;
	ceph_msg_connect connect;
	ceph_msg_connect_reply reply;
	bufferptr bp;
	bufferlist authorizer, authorizer_reply;
	bool authorizer_valid;
	uint64_t feat_missing;
	bool replaced = false;
	bool is_reset_from_peer = false;
	CryptoKey session_key;
	int removed; // single-use down below
    	entity_addr_t peer_addr;
    	__u32 connect_seq, peer_global_seq, global_seq = 0;
  	int reply_tag = 0;
  	uint64_t existing_seq = -1;
    	Messenger::Policy policy;

  	set_socket_options(accept_sd);

  	// announce myself.
	r = tcp_write(CEPH_BANNER, strlen(CEPH_BANNER));
	if (r < 0) {
		printf("accept couldn't write banner\n");
		goto err_ret;
	}

	// and my addr
	len = sizeof(socket_addr.ss_addr());
	r = ::getpeername(accept_sd, (sockaddr*)&socket_addr.ss_addr(), &len);
	if (r < 0) {
		printf("accept  failed to getpeername \n");
		goto err_ret;
	}
	::encode(socket_addr, addrs);
  	r = tcp_write(addrs.c_str(), addrs.length());
	if (r < 0) {
		printf("accep faild to getpeername\n");
		goto err_ret;
	}

	// identify peer
	if (tcp_read(banner, strlen(CEPH_BANNER)) < 0) {
		printf("accept couldn't read banner\n");
		goto err_ret;
	}
	if (memcmp(banner, CEPH_BANNER, strlen(CEPH_BANNER))) {
		banner[strlen(CEPH_BANNER)] = 0;
		printf("accept peer sent bad banner %s, need: %s\n",  banner, CEPH_BANNER);
		goto err_ret;
	}

	{
		bufferptr tp(sizeof(peer_addr));
		addrbl.push_back(std::move(tp));
	}
	if (tcp_read(addrbl.c_str(), addrbl.length()) < 0) {
		printf("accept couldn't read peer_addr\n");
		goto err_ret;
	}

	while (1) {
		if (tcp_read((char*)&connect, sizeof(connect)) < 0) {
			printf("accept couldn't read connect\n");
			goto err_ret;
		}

    		connect.features = ceph_sanitize_features(connect.features);
		authorizer.clear();
		if (connect.authorizer_len) {
			bp = buffer::create(connect.authorizer_len);
			if (tcp_read(bp.c_str(), connect.authorizer_len) < 0) {
				printf("accept couldn't read connect authorizer\n");
				goto err_ret;
			}
			authorizer.push_back(std::move(bp));
			authorizer_reply.clear();
		}


		//cluster_protocol(0),
		memset(&reply, 0, sizeof(reply));
		reply.protocol_version = 0;
		if (connect.protocol_version != reply.protocol_version) {
			reply.tag = CEPH_MSGR_TAG_BADPROTOVER;
			goto reply;
		}

		connect_seq = connect.connect_seq + 1;
		peer_global_seq = connect.global_seq;
		//assert(state == STATE_ACCEPTING);
		//state = STATE_OPEN;
		reply.tag = (reply_tag ? reply_tag : CEPH_MSGR_TAG_READY);
		reply.features = policy.features_supported;
		reply.global_seq = ++global_seq;
		reply.connect_seq = connect_seq;
		reply.flags = 0;
		reply.authorizer_len = authorizer_reply.length();

  		connection_state->set_features((uint64_t)reply.features & (uint64_t)connect.features);

		r = tcp_write((char*)&reply, sizeof(reply));
		if (r < 0) {
			ret = -r;
			printf("write reply error\n");
			goto err_ret;
		}

		if (reply.authorizer_len) {
			r = tcp_write(authorizer_reply.c_str(), authorizer_reply.length());
			if (r < 0) {
				ret = -r;
				printf("write authorizer error\n");
				goto err_ret;
			}
		}

#if 0
		if (reply_tag == CEPH_MSGR_TAG_SEQ) {
			if (tcp_write((char*)&existing_seq, sizeof(existing_seq)) < 0) {
				ldout(msgr->cct,2) << "accept write error on in_seq" << dendl;
				goto fail_registered;
			}
			if (tcp_read((char*)&newly_acked_seq, sizeof(newly_acked_seq)) < 0) {
				ldout(msgr->cct,2) << "accept read error on newly_acked_seq" << dendl;
				goto fail_registered;
			}
		}
#endif

		return 0;
	}

	return 0;
err_ret:
	return ret;
}

void *__reader(void *_arg)
{
	int accept_sd;

	accept_sd = *((int *)_arg);

	ret = accept(accept_sd);
	if (ret) {
		printf("accept error %d \n", ret);
		goto err_ret;
	}

	while (1) {
		char tag = -1;
		printf("reader reading tag...\n");
		if (tcp_read((char*)&tag, 1) < 0) {
			printf("reader couldn't read tag, \n");
			goto err_ret;
		}

		if (tag == CEPH_MSGR_TAG_KEEPALIVE) {
			printf("reader got KEEPALIVE, \n");
			//connection_state->set_last_keepalive(ceph_clock_now(NULL));
			continue;
		}

		if (tag == CEPH_MSGR_TAG_KEEPALIVE2) {
			printf("reader got KEEPALIVE2 tag ...\n");
			ceph_timespec t;
			int rc = tcp_read((char*)&t, sizeof(t));
			if (rc < 0) {
				printf("reader couldn't read KEEPALIVE2 stamp %s\n", cpp_strerror(errno));
				goto err_ret;
			} else {
				//send_keepalive_ack = true;
				//keepalive_ack_stamp = utime_t(t);
				//ldout(msgr->cct,2) << "reader got KEEPALIVE2 " << keepalive_ack_stamp
					//<< dendl;
				//connection_state->set_last_keepalive(ceph_clock_now(NULL));
			}
			continue;
		}

		if (tag == CEPH_MSGR_TAG_KEEPALIVE2_ACK) {
			pritnf("reader got KEEPALIVE_ACK\n");
			struct ceph_timespec t;
			int rc = tcp_read((char*)&t, sizeof(t));
			if (rc < 0) {
				printf("reader couldn't read KEEPALIVE2 stamp %s\n", cpp_strerror(errno));
				goto err_ret;
			} else {
				//connection_state->set_last_keepalive_ack(utime_t(t));
			}
			continue;
		}

		if (tag == CEPH_MSGR_TAG_ACK) {
			printf("reader got ACK\n");
			ceph_le64 seq;
			int rc = tcp_read((char*)&seq, sizeof(seq));
			pipe_lock.Lock();
			if (rc < 0) {
				printf("reader couldn't read ack seq, %s\n", cpp_strerror(errno));
				goto err_ret;
			}
		       	//else if (state != STATE_CLOSED) {
				//handle_ack(seq);
			//}
			continue;
		} else if (tag == CEPH_MSGR_TAG_MSG) {
			printf("reader got MSG\n");
      			Message *m = 0;
      			int r = read_message(&m, auth_handler.get());
			if (!m) {
				if (r<0) {
					goto err_ret;
				}
				continue;
			}

			//todo 处理seq
      			if (m->get_seq() <= in_seq) {
			}
      			if (m->get_seq() > in_seq + 1) {
			}

			ret = fetch_msg(m, accept_sd);
			if (ret) {
				goto err_ret;
			}
		} else if (tag == CEPH_MSGR_TAG_CLOSE) {
      			printf("reader got CLOSE\n");
			break;
		} else {
      			printf("reader bad tag \n");
			assert(0);
		}

	}

	return NULL;
err_ret:
	return NULL
}

void *__server_start(void *_arg)
{
	int ret, socket_fd, listen_port, accept_fd;
	sockaddr_in server_addr;
	int *arg;
	pthread_t th;

	listen_port = *((int *)_arg);

	memset(&server_addr, 0, sizeof(server_addr));  
	server_addr.sin_family = AF_INET;  
	server_addr.sin_addr.s_addr = htonl(INADDR_ANY);  
	server_addr.sin_port = htons(listen_port); 

	socket_fd = ::socket(AF_INET, SOCK_STREAM, 0);
	if (socket_fd < 0) {
		dout(0) << "socket create error" << dendl;
		goto err_ret;
	}

	ret = ::bind(socket_fd, &server_addr, sizeof(server_addr));
	if (ret < 0) {
		dout(0) << "bind socket error" << dendl;
		ret = -errno;
		goto err_ret;
	}

	ret = ::listen(socket_fd, 128);
	if (ret < 0) {
		ret = -errno;
		dout(0) << "listen socket error, ret " << ret << dendl;
		goto err_ret;
	}

	while (1) {
		entity_addr_t addr;
		socklen_t slen = sizeof(addr.ss_addr());
		accept_sd = ::accept(socket_fd, (sockaddr*)&addr.ss_addr(), &slen);
		if (accept_sd < 0) {
			ret = -errno;
			dout(0) << "listen socket error, ret " << ret << dendl;
			goto err_ret;
		}

		arg = malloc(sizeof(*arg));
		*arg = accept_sd;
		ret = pthread_create(&th, NULL, __reader, (void *)arg);
		if (ret) {
			dout(0) << "thread create error, ret " << ret << dendl;
			goto err_ret;
		}
	}

	return 0;
	return NULL;
err_ret:
	return ret;
	return NULL;
}

int server_start(int listen_port)
{
	int ret;
	int *arg;
	pthread_t th;

	arg = malloc(sizeof(*arg));
	*arg = listen_port;

	ret = pthread_create(&th, NULL, __server_start, (void *)arg);
	if (ret) {
		dout(0) << "thread create error, ret " << ret << dendl;
		goto err_ret;
	}

	return 0;
err_ret:
	return ret;
}

int main(int argc, const char **argv) 
{
	int ret;

	ret = server_start(6790);
	if (ret) {
		goto err_ret;
	}

	ret = server_start(6791);
	if (ret) {
		goto err_ret;
	}

	pthread_exit(NULL);

	return 0;
err_ret:
	return ret;
}
