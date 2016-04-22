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

Message *fetch_msg(Message *m, Message *rsp_m)
{
	printf("msg\n");
	return m;
}

int tcp_read(char *buf, unsigned len, int accept_sd)
{
	int ret;
	ssize_t got;

	while (len > 0) {
again:
		got = ::recv(accept_sd, buf, len, MSG_WAITALL);
		if (got < 0) {
			if (errno == EAGAIN || errno == EINTR) {
				goto again;
			}
			printf("recv error\n");
			ret = got;
			goto err_ret;
		}

		len -= got;
    		buf += got;
	}

	return 0;
err_ret:
	return ret;
}

int tcp_write(const char *buf, unsigned len, int accept_sd)
{
    	int did, ret;
	while (len > 0) {
    		did = ::send(accept_sd, buf, len, MSG_NOSIGNAL);
    		if (did < 0) {
			ret = did;
			goto err_ret;
		}

		len -= did;
		buf += did;
	}

	return 0;
err_ret:
	return ret;
}


int do_sendmsg(struct msghdr *msg, unsigned len, bool more, int accept_sd)
{
	int r, ret;

	while (len > 0) {
    		r = ::sendmsg(accept_sd, msg, MSG_NOSIGNAL | (more ? MSG_MORE : 0));
    		if (r < 0) { 
			ret = -1;
			printf("write ack, sendmsg error\n");
			goto err_ret;
		}

		len -= r;
		if (len == 0) break;

		while (r > 0) {
			if (msg->msg_iov[0].iov_len <= (size_t)r) {
				r -= msg->msg_iov[0].iov_len;
				msg->msg_iov++;
				msg->msg_iovlen--;
			} else {
				msg->msg_iov[0].iov_base = (char *)msg->msg_iov[0].iov_base + r;
				msg->msg_iov[0].iov_len -= r;
				break;
			}
		}
	}

	return 0;
err_ret:
	return ret;
}

int write_keepalive2(char tag, const utime_t& t, int accept_sd)
{
	int ret;
	struct ceph_timespec ts;
	struct msghdr msg;
	struct iovec msgvec[2];

	msgvec[0].iov_base = &tag;
	msgvec[0].iov_len = 1;
	msgvec[1].iov_base = &ts;
	msgvec[1].iov_len = sizeof(ts);

	memset(&msg, 0, sizeof(msg));
	msg.msg_iov = msgvec;
	msg.msg_iovlen = 2;
	t.encode_timeval(&ts);
	if (do_sendmsg(&msg, 1 + sizeof(ts), false) < 0) {
		ret = -1;
		goto err_ret;
	}

	return 0;
err_ret:
	return ret;
}

int write_ack(uint64_t seq, int accept_sd)
{
	int ret, len;
	char c = CEPH_MSGR_TAG_ACK;
	ceph_le64 s;
	struct msghdr msg;
	struct iovec msgvec[2];
	s = seq;

	memset(&msg, 0, sizeof(msg));
	msgvec[0].iov_base = &c;
	msgvec[0].iov_len = 1;
	msgvec[1].iov_base = &s;
	msgvec[1].iov_len = sizeof(s);
	msg.msg_iov = msgvec;
	msg.msg_iovlen = 2;

	len = sizeof(s) + 1;
	if (do_sendmsg(msg, len, true, accept_sd) < 0) {
		ret = -1;
		goto err_ret;
	}

	return 0;
err_ret:
	return ret;
}

int read_message(Message **pm, PipeConnectionRef connection_state, int accept_sd)
{
	int ret = -1;
	ceph_msg_header header; 
	ceph_msg_footer footer;
	__u32 header_crc = 0;
    	ceph_msg_header_old oldheader;

	bufferlist front, middle, data;
	int front_len, middle_len;
	unsigned data_len, data_off;
	int aborted;
	Message *message;
	utime_t recv_stamp = ceph_clock_now(msgr->cct);
	uint64_t message_size;
	utime_t throttle_stamp;

	if (connection_state->has_feature(CEPH_FEATURE_NOSRCADDR)) {
		if (tcp_read((char*)&header, sizeof(header)) < 0) {
			ret = -1;
			printf("read header error\n");;
			goto err_ret;
		}
		header_crc = ceph_crc32c(0, (unsigned char *)&header, sizeof(header) - sizeof(header.crc));
	} else {
		if (tcp_read((char*)&oldheader, sizeof(oldheader)) < 0) {
			ret = -1;
			printf("read oldheader error\n");;
			goto err_ret;
		}

		memcpy(&header, &oldheader, sizeof(header));
		header.src = oldheader.src.name;
		header.reserved = oldheader.reserved;
		header.crc = oldheader.crc;
		header_crc = ceph_crc32c(0, (unsigned char *)&oldheader, sizeof(oldheader) - sizeof(oldheader.crc));
	}

  	message_size = header.front_len + header.middle_len + header.data_len;
  	if (message_size) {
	}
	throttle_stamp = ceph_clock_now(msgr->cct);

  	// read front
	front_len = header.front_len;
	if (front_len) {
		bufferptr bp = buffer::create(front_len);
		if (tcp_read(bp.c_str(), front_len) < 0) {
			printf("read front error\n");
			ret = -1;
			goto err_ret;
		}
		front.push_back(std::move(bp));
		printf("reader got front %d\n", front.length());
	}

  	// read middle
	middle_len = header.middle_len;
	if (middle_len) {
		bufferptr bp = buffer::create(middle_len);
		if (tcp_read(bp.c_str(), middle_len) < 0) {
			printf("read middle error\n");
			ret = -1;
			goto ret;
		}
		middle.push_back(std::move(bp));
		printf("reader got middle %d\n", middle.length());
	}

	// read data
	data_len = le32_to_cpu(header.data_len);
	data_off = le32_to_cpu(header.data_off);
	if (data_len) {
		unsigned offset = 0;
		unsigned left = data_len;

		bufferlist newbuf, rxbuf;
		bufferlist::iterator blp;
		int rxbuf_version = 0;

		while (left > 0) {
			// wait for data
			if (tcp_read_wait() < 0)
				goto out_dethrottle;

			// get a buffer
			connection_state->lock.Lock();
			map<ceph_tid_t,pair<bufferlist,int> >::iterator p = connection_state->rx_buffers.find(header.tid);
			if (p != connection_state->rx_buffers.end()) {
				if (rxbuf.length() == 0 || p->second.second != rxbuf_version) {
					cout << "reader seleting rx buffer v " << p->second.second
						<< " at offset " << offset
						<< " len " << p->second.first.length() << endl;
					rxbuf = p->second.first;
					rxbuf_version = p->second.second;
					// make sure it's big enough
					if (rxbuf.length() < data_len)
						rxbuf.push_back(buffer::create(data_len - rxbuf.length()));
					blp = p->second.first.begin();
					blp.advance(offset);
				}
			} else {
				if (!newbuf.length()) {
					cout(msgr->cct,20) << "reader allocating new rx buffer at offset " << offset << endl;
					alloc_aligned_buffer(newbuf, data_len, data_off);
					blp = newbuf.begin();
					blp.advance(offset);
				}
			}
			bufferptr bp = blp.get_current_ptr();
			int read = MIN(bp.length(), left);
			cout << "reader reading nonblocking into " << (void*)bp.c_str() << " len " << bp.length() << endl;
			ssize_t got = tcp_read(bp.c_str(), read, accept_sd);
			cout(msgr->cct,30) << "reader read " << got << " of " << read << endl;
			connection_state->lock.Unlock();
			if (got < 0) {
				cout << "tcp read bp err" << endl;
				ret = -1;
				goto err_ret;
			}

			if (got > 0) {
				blp.advance(got);
				data.append(bp, 0, got);
				offset += got;
				left -= got;
			} // else we got a signal or something; just loop.
		}
	}

	// footer
	if (connection_state->has_feature(CEPH_FEATURE_MSG_AUTH)) {
		if (tcp_read((char*)&footer, sizeof(footer)) < 0) {
			ret = -1;
			cout << "read footer error" << endl;
			goto err_ret;
		}
	} else {
		ceph_msg_footer_old old_footer;
		if (tcp_read((char*)&old_footer, sizeof(old_footer)) < 0) {
			ret = -1;
			cout << "read footer error" << endl;
			goto err_ret;
		}
		footer.front_crc = old_footer.front_crc;
		footer.middle_crc = old_footer.middle_crc;
		footer.data_crc = old_footer.data_crc;
		footer.sig = 0;
		footer.flags = old_footer.flags;
	}

	aborted = (footer.flags & CEPH_MSG_FOOTER_COMPLETE) == 0;
	cout << "aborted = " << aborted << endl;
	if (aborted) {
		cout << "reader got " << front.length() << " + " << middle.length() << " + " << data.length()
			<< " byte message.. ABORTED" << endl;
		ret = 0;
		goto out;
	}

	//msgr ? 怎么获取。总是用到
	//
	//msgr->ctt = NULL;
	//crcflags = 0;
	message = decode_message(NULL, 0, header, footer, front, middle, data);
	if (!message) {
		ret = -EINVAL;
		goto err_ret;
	}

  	message->set_recv_stamp(recv_stamp);
  	message->set_recv_complete_stamp(ceph_clock_now(msgr->cct));

  	*pm = message;
out:
	return 0;
err_ret:
	return ret;
}

#if 0
int write_message(const ceph_msg_header& h, const ceph_msg_footer& f, bufferlist& body)
{
	return 0;
}
#endif

//return -1, if error
int write_message(Message **pm, int accept_sd)
{
	return 0;
}

int set_socket_options(int sd)
{
	(void)sd;
	return 0;
}

int accept(int accept_sd, PipeConnectionRef connection_state)
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
    	//Messenger::Policy policy;

  	set_socket_options(accept_sd);

  	// announce myself.
	r = tcp_write(CEPH_BANNER, strlen(CEPH_BANNER), accept_sd);
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
  	r = tcp_write(addrs.c_str(), addrs.length(), accept_sd);
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
	if (tcp_read(addrbl.c_str(), addrbl.length(), accept_sd) < 0) {
		printf("accept couldn't read peer_addr\n");
		goto err_ret;
	}

	while (1) {
		if (tcp_read((char*)&connect, sizeof(connect), accept_sd) < 0) {
			printf("accept couldn't read connect\n");
			goto err_ret;
		}

    		connect.features = ceph_sanitize_features(connect.features);
		authorizer.clear();
		if (connect.authorizer_len) {
			bp = buffer::create(connect.authorizer_len);
			if (tcp_read(bp.c_str(), connect.authorizer_len, accept_sd) < 0) {
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

		uint64_t supported =
			CEPH_FEATURE_UID |
			CEPH_FEATURE_NOSRCADDR |
			DEPRECATED_CEPH_FEATURE_MONCLOCKCHECK |
			CEPH_FEATURE_PGID64 |
			CEPH_FEATURE_MSG_AUTH;

		reply.tag = (reply_tag ? reply_tag : CEPH_MSGR_TAG_READY);
		reply.features = supported;
		reply.global_seq = ++global_seq;
		reply.connect_seq = connect_seq;
		reply.flags = 0;
		reply.authorizer_len = authorizer_reply.length();

  		connection_state->set_features((uint64_t)reply.features & (uint64_t)connect.features);

		r = tcp_write((char*)&reply, sizeof(reply), accept_sd);
		if (r < 0) {
			ret = -r;
			printf("write reply error\n");
			goto err_ret;
		}

		if (reply.authorizer_len) {
			printf("authorizer len, crazy , i do not need authorizer\n");
			assert(0);
			r = tcp_write(authorizer_reply.c_str(), authorizer_reply.length(), accept_sd);
			if (r < 0) {
				ret = -r;
				printf("write authorizer error\n");
				goto err_ret;
			}
		}

#if 0
		if (reply_tag == CEPH_MSGR_TAG_SEQ) {
			if (tcp_write((char*)&existing_seq, sizeof(existing_seq)) < 0, accept_sd) {
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
	int accept_sd, rc;
	uint64_t in_seq = 0, out_seq = 0; //todo 是否会溢出
	utime_t keepalive_ack_stamp;
	struct ceph_timespec t;
	char tag = -1;
	ceph_le64 seq;
	PipeConnectionRef connection_state;

	accept_sd = *((int *)_arg);

	ret = accept(accept_sd, connection_state);
	if (ret) {
		printf("accept error %d \n", ret);
		goto err_ret;
	}

	while (1) {
		printf("reader reading tag...\n");
		if (tcp_read((char*)&tag, 1, accept_sd) < 0) {
			printf("reader couldn't read tag, \n");
			goto err_ret;
		}

		if (tag == CEPH_MSGR_TAG_KEEPALIVE) {
			printf("reader got KEEPALIVE, \n");
			connection_state->set_last_keepalive(ceph_clock_now(NULL));
			continue;
		}

		if (tag == CEPH_MSGR_TAG_KEEPALIVE2) {
			printf("reader got KEEPALIVE2 tag ...\n");
			ceph_timespec t;
			ret = tcp_read((char*)&t, sizeof(t), accept_sd);
			if (ret < 0) {
				printf("reader couldn't read KEEPALIVE2 stamp %s\n", cpp_strerror(errno));
				goto err_ret;
			} else {
				keepalive_ack_stamp = utime_t(t);
				connection_state->set_last_keepalive(ceph_clock_now(NULL));
				ret = write_keepalive2(CEPH_MSGR_TAG_KEEPALIVE2_ACK, t, accept_sd);
				if (ret) {
					printf("write keepalive2 error %d\n", ret);
					goto ret;
				}
			}
			continue;
		}

		if (tag == CEPH_MSGR_TAG_KEEPALIVE2_ACK) {
			pritnf("reader got KEEPALIVE_ACK\n");
			ret = tcp_read((char*)&t, sizeof(t), accept_sd);
			if (ret < 0) {
				printf("reader couldn't read KEEPALIVE2 stamp %s\n", cpp_strerror(errno));
				goto err_ret;
			} else {
				connection_state->set_last_keepalive_ack(utime_t(t));
			}
			continue;
		}

		if (tag == CEPH_MSGR_TAG_ACK) {
			printf("reader got ACK\n");
			ret = tcp_read((char*)&seq, sizeof(seq), accept_sd);
			if (ret < 0) {
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
      			ret = read_message(&m, connection_state, accept_sd);
			if (!m) {
				if (ret<0) {
					goto err_ret;
				}
				continue;
			}

			//todo 处理seq
      			if (m->get_seq() <= in_seq) {
				printf("reader old msg\n");
				continue;
			}
      			if (m->get_seq() > in_seq + 1) {
	  			assert(0 == "skipped incoming seq");
			}

      			m->set_connection(connection_state.get());
      			in_seq = m->get_seq();
			write_ack(in_seq, accept_sd);

      			Message *rsp_m = 0;
			ret = fetch_msg(m, rsp_m);
			if (ret) {
				goto err_ret;
			}

			rsp_m->set_seq(++out_seq);
			ret = write_msg(rsp_m, accept_sd);
			if (ret) {
				goto err_ret;
			}
		} else if (tag == CEPH_MSGR_TAG_CLOSE) {
      			printf("reader got CLOSE\n");
			goto err_ret;
			break;
		} else {
      			printf("reader bad tag \n");
        		::shutdown(accept_sd, SHUT_RDWR);
			assert(0);
		}

	}

	return NULL;
err_ret:
        ::shutdown(accept_sd, SHUT_RDWR);
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
