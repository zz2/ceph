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
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <sys/uio.h>
#include <limits.h>
#include <poll.h>
#include <iostream>
#include <string>
#include <arpa/inet.h>

using namespace std;

#include "common/config.h"
#include "include/ceph_features.h"

#include "mon/MonMap.h"
#include "mon/Monitor.h"
#include "mon/MonitorDBStore.h"
#include "mon/MonClient.h"

#include "msg/Messenger.h"
#include "include/CompatSet.h"
#include "msg/simple/PipeConnection.h"


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

typedef struct {
	int accept_sd;
    	size_t recv_ofs;
    	size_t recv_len;
	int ms_tcp_read_timeout = 900;//秒
	int timeout = 900000;//毫秒
} pipe_t;

#if 0
Message *fetch_msg(Message *m, Message *rsp_m)
{
	printf("msg\n");
	return m;
}
#endif

int tcp_read_wait(pipe_t *pipe)
{
	struct pollfd pfd;
	short evmask;
	pfd.fd = pipe->accept_sd;
	pfd.events = POLLIN;
#if defined(__linux__)
	pfd.events |= POLLRDHUP;
#endif

	if (pipe->accept_sd < 0)
		return -1;

	if (pipe->recv_len > pipe->recv_ofs) {
		return 0;
	}

	if (poll(&pfd, 1, pipe->timeout) <= 0) {
		printf("pipe timeout\n");
		return -1;
	}

	evmask = POLLERR | POLLHUP | POLLNVAL;
#if defined(__linux__)
	evmask |= POLLRDHUP;
#endif

	if (pfd.revents & POLLRDHUP) {
		printf("POLLRDHUP\n"); return -1;
	}

	if (pfd.revents & POLLERR) {
		printf("pollerr\n"); return -1;
	}
	if (pfd.revents & POLLHUP) {
		printf("pollHUP\n"); return -1;
	}

	if (pfd.revents & POLLNVAL) {
		printf("POLLHUP\n"); return -1;
	}

	if (pfd.revents & evmask) {
		printf("pipe revents error, hup, nval, %d\n", pfd.revents);
		return -1;
	}

	if (!(pfd.revents & POLLIN)) {
		printf("pipe revents not poolin\n");
		return -1;
	}

	return 0;
}

static void alloc_aligned_buffer(bufferlist& data, unsigned len, unsigned off)
{
  // create a buffer to read into that matches the data alignment
  unsigned left = len;
  if (off & ~CEPH_PAGE_MASK) {
    // head
    unsigned head = 0;
    head = MIN(CEPH_PAGE_SIZE - (off & ~CEPH_PAGE_MASK), left);
    data.push_back(buffer::create(head));
    left -= head;
  }
  unsigned middle = left & CEPH_PAGE_MASK;
  if (middle > 0) {
    data.push_back(buffer::create_page_aligned(middle));
    left -= middle;
  }
  if (left) {
    data.push_back(buffer::create(left));
  }
}

int tcp_read_nonblocking(char *buf, unsigned len, pipe_t *pipe)
{
	int ret;
	ssize_t got;

again:
	got = ::recv(pipe->accept_sd, buf, len, MSG_DONTWAIT);
	if (got < 0) {
		if (errno == EAGAIN || errno == EINTR) {
			goto again;
		}
		printf("recv error\n");
		ret = got;
		goto err_ret;
	}
	printf("recv got : %d\n", (int)got);

	return got;
err_ret:
	return ret;
}

int tcp_read(char *buf, unsigned len, pipe_t *pipe)
{
	int ret;
	ssize_t got;

	if (pipe->accept_sd < 0) {
		ret = -1;
		printf("pipe->accept_sd < 0\n");
		goto err_ret;
	}

	while (len > 0) {
		if (tcp_read_wait(pipe) < 0) {
			ret = -1;
			goto err_ret;
		}

		got = tcp_read_nonblocking(buf, len, pipe);
		if (got < 0) {
			ret = -1;
			goto err_ret;
		}

		if (got == 0) {
			printf("poll() said there was data, but we didn't read any - peer\n");
			ret = -1;
			goto err_ret;
		}

		printf("recv got : %d\n", (int)got);

		len -= got;
    		buf += got;
	}

	return 0;
err_ret:
	return ret;
}

int tcp_write(const char *buf, unsigned len, pipe_t *pipe)
{
	int did, ret;
	struct pollfd pfd;

	pfd.fd = pipe->accept_sd;
	pfd.events = POLLOUT | POLLHUP | POLLNVAL | POLLERR;
#if defined(__linux__)
	pfd.events |= POLLRDHUP;
#endif

	if (pipe->accept_sd < 0)
		return -1;

	if (poll(&pfd, 1, -1) < 0)
		return -1;

	if (!(pfd.revents & POLLOUT))
		return -1;

  	assert(len > 0);
	while (len > 0) {
		did = ::send(pipe->accept_sd, buf, len, MSG_NOSIGNAL);
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
	if (do_sendmsg(&msg, 1 + sizeof(ts), false, accept_sd) < 0) {
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
	if (do_sendmsg(&msg, len, true, accept_sd) < 0) {
		ret = -1;
		goto err_ret;
	}

	return 0;
err_ret:
	return ret;
}

int read_message(Message **pm, uint64_t *supported, pipe_t *pipe)
{
	int ret = -1;
	ceph_msg_header header; 
	ceph_msg_footer footer;
	__u32 header_crc;

	bufferlist front, middle, data;
	int front_len, middle_len;
	unsigned data_len, data_off;
	int aborted;
	Message *message;
	utime_t recv_stamp = ceph_clock_now(NULL);
	uint64_t message_size;
	utime_t throttle_stamp;
    	ceph_msg_header_old oldheader;

	header_crc = 0;
	if (*supported & CEPH_FEATURE_NOSRCADDR) {
		if (tcp_read((char*)&header, sizeof(header), pipe) < 0) {
			ret = -1;
			printf("read header error\n");;
			goto err_ret;
		}
		header_crc = ceph_crc32c(0, (unsigned char *)&header, sizeof(header) - sizeof(header.crc));
	} else {
		if (tcp_read((char*)&oldheader, sizeof(oldheader), pipe) < 0) {
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
	throttle_stamp = ceph_clock_now(NULL);

  	// read front
	front_len = header.front_len;
	if (front_len) {
		bufferptr bp = buffer::create(front_len);
		if (tcp_read(bp.c_str(), front_len, pipe) < 0) {
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
		if (tcp_read(bp.c_str(), middle_len, pipe) < 0) {
			printf("read middle error\n");
			ret = -1;
			goto err_ret;
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
		//int rxbuf_version = 0;

		while (left > 0) {
			// wait for data
			//if (tcp_read_wait() < 0)

			// get a buffer
			if (!newbuf.length()) {
				printf("reader allocating new rx buffer at offset %d \n", (int)offset);
				alloc_aligned_buffer(newbuf, data_len, data_off);
				blp = newbuf.begin();
				blp.advance(offset);
			}
			bufferptr bp = blp.get_current_ptr();

			int read = MIN(bp.length(), left);
			//ldout(NULL, -1) << "reader reading nonblocking into " << (void*)bp.c_str() << " len " << bp.length() << dendl;
			ssize_t got = tcp_read(bp.c_str(), read, pipe);
			//ldout(NULL, -1) << "reader read " << got << " of " << read << dendl;
			if (got < 0) {
				printf("tcp read bp err\n");
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
	if (*supported & CEPH_FEATURE_MSG_AUTH) {
		if (tcp_read((char*)&footer, sizeof(footer), pipe) < 0) {
			ret = -1;
			printf("read footer error\n");
			goto err_ret;
		}
	} else {
		ceph_msg_footer_old old_footer;
		if (tcp_read((char*)&old_footer, sizeof(old_footer), pipe) < 0) {
			ret = -1;
			printf("read footer error\n");
			goto err_ret;
		}
		footer.front_crc = old_footer.front_crc;
		footer.middle_crc = old_footer.middle_crc;
		footer.data_crc = old_footer.data_crc;
		footer.sig = 0;
		footer.flags = old_footer.flags;
	}

	aborted = (footer.flags & CEPH_MSG_FOOTER_COMPLETE) == 0;
	//ldout(NULL, -1) << "aborted = " << aborted << dendl;
	if (aborted) {
		//ldout(NULL, -1) << "reader got " << front.length() << " + " << middle.length() << " + " << data.length()
			//<< " byte message.. ABORTED" << dendl;
		printf("aborted\n");
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
  	message->set_recv_complete_stamp(ceph_clock_now(NULL));

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

int accept(pipe_t *pipe, uint64_t *supported)
{
	int ret;
	// vars
	bufferlist addrs;
	entity_addr_t socket_addr, socket_addr_local;
	socklen_t len;
	int r;
	char banner[strlen(CEPH_BANNER)+1];
	bufferlist addrbl;
	ceph_msg_connect connect;
	ceph_msg_connect_reply reply;
	bufferptr bp;
	bufferlist authorizer, authorizer_reply;
	//bool authorizer_valid;
	//uint64_t feat_missing;
	//bool replaced = false;
	//bool is_reset_from_peer = false;
	CryptoKey session_key;
	//int removed; // single-use down below
    	entity_addr_t peer_addr;
    	__u32 connect_seq, global_seq, peer_global_seq, global_se_;
  	int reply_tag = 0;
  	//uint64_t existing_seq = -1;
    	//Messenger::Policy policy;
	
	global_seq = 0;
	peer_global_seq = 0;
	global_se_ = 0;
  	set_socket_options(pipe->accept_sd);

  	// announce myself.
	printf("announce myself\n");
	r = tcp_write(CEPH_BANNER, strlen(CEPH_BANNER), pipe);
	if (r < 0) {
		printf("accept couldn't write banner\n");
		ret = errno;
		goto err_ret;
	}

	// and my addr
	printf("add my addr\n");
	char serv_ip[20], guest_ip[20];
	//((sockaddr_in)(socket_addr_local.ss_addr())).sin_family = AF_INET; 
	//socket_addr_local.ss_addr().sin_addr.s_addr = htonl(INADDR_ANY); 
	//socket_addr_local.ss_addr().sin_addr.s_addr = inet_addr("192.168.120.31"); 
	//socket_addr_local.ss_addr().sin_port = htons(6789);; 
	len = sizeof(socket_addr.ss_addr());
	r = ::getsockname(pipe->accept_sd, (sockaddr*)&socket_addr_local.ss_addr(), &len);
	if (r < 0) {
		printf("accept  failed to getpeername \n");
		ret = errno;
		goto err_ret;
	}
	::encode(socket_addr_local, addrs);

	r = ::getpeername(pipe->accept_sd, (sockaddr*)&socket_addr.ss_addr(), &len);
	if (r < 0) {
		printf("accept  failed to getpeername \n");
		ret = errno;
		goto err_ret;
	}
	::encode(socket_addr, addrs);
  	r = tcp_write(addrs.c_str(), addrs.length(), pipe);
	if (r < 0) {
		printf("accep faild to getpeername\n");
		ret = errno;
		goto err_ret;
	}
	//inet_ntop(AF_INET, &((sockaddr*)(&socket_addr_local.ss_addr())->sin_addr), serv_ip, sizeof(serv_ip));
	//inet_ntop(AF_INET, &((sockaddr*)(&socket_addr.ss_addr())->sin_addr), guest_ip, sizeof(serv_ip));
	//printf("host: %s, guest: %s\n", serv_ip, guest_ip);
	//printf("read banner\n");
	// identify peer
	if (tcp_read(banner, strlen(CEPH_BANNER), pipe) < 0) {
		printf("accept couldn't read banner\n");
		ret = 1;
		goto err_ret;
	}
	if (memcmp(banner, CEPH_BANNER, strlen(CEPH_BANNER))) {
		banner[strlen(CEPH_BANNER)] = 0;
		printf("accept peer sent bad banner %s, need: %s\n",  banner, CEPH_BANNER);
		ret = 1;
		goto err_ret;
	}
	printf("read banner, %s\n", banner);

	{
		bufferptr tp(sizeof(peer_addr));
		addrbl.push_back(std::move(tp));
	}

	printf("read peer addr\n");
	if (tcp_read(addrbl.c_str(), addrbl.length(), pipe) < 0) {
		ret = 1;
		printf("accept couldn't read peer_addr, %d\n", ret);
		goto err_ret;
	}

	while (1) {
		printf("read connect\n");
		if (tcp_read((char*)&connect, sizeof(connect), pipe) < 0) {
			printf("accept couldn't read connect\n");
			ret = 1;
			goto err_ret;
		}

    		connect.features = ceph_sanitize_features(connect.features);
		authorizer.clear();
		if (connect.authorizer_len) {
			printf("read auth\n");
			bp = buffer::create(connect.authorizer_len);
			if (tcp_read(bp.c_str(), connect.authorizer_len, pipe) < 0) {
				printf("accept couldn't read connect authorizer\n");
				ret = 1;
				goto err_ret;
			}
			authorizer.push_back(std::move(bp));
			authorizer_reply.clear();
		}

		//cluster_protocol(0),
		memset(&reply, 0, sizeof(reply));
		reply.protocol_version = 15;
		if (connect.protocol_version != reply.protocol_version) {
			reply.tag = CEPH_MSGR_TAG_BADPROTOVER;
			printf("CEPH_MSGR_TAG_BADPROTOVER, connect: %d,  reply: %d\n", connect.protocol_version, reply.protocol_version);
		}

    		if (connect.authorizer_protocol == CEPH_AUTH_CEPHX) {
			printf("fuck\n");
			exit(1);
		}

		connect_seq = connect.connect_seq + 1;
		peer_global_seq = connect.global_seq;
		//assert(state == STATE_ACCEPTING);
		//state = STATE_OPEN;

		reply.tag = (reply_tag ? reply_tag : CEPH_MSGR_TAG_READY);
		reply.features = (*supported) * ((uint64_t)connect.features);
		reply.global_seq = ++global_seq;
		reply.connect_seq = connect_seq;
		reply.flags = 0;
		reply.authorizer_len = authorizer_reply.length();

		printf("send reply\n");
		r = tcp_write((char*)&reply, sizeof(reply), pipe);
		if (r < 0) {
			ret = -r;
			printf("write reply error\n");
			goto err_ret;
		}

		if (reply.authorizer_len) {
			printf("authorizer len, crazy , i do not need authorizer\n");
			assert(0);
			r = tcp_write(authorizer_reply.c_str(), authorizer_reply.length(), pipe);
			if (r < 0) {
				ret = -r;
				printf("write authorizer error\n");
				goto err_ret;
			}
		}

		break;
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
	}

	return 0;
err_ret:
	return ret;
}

void *__reader(void *_arg)
{
	int ret, accept_sd;
       	//todo 是否会溢出
	uint64_t in_seq, out_seq;
	utime_t keepalive_ack_stamp;
	struct ceph_timespec t;
	char tag = -1;
	ceph_le64 seq;
	pipe_t pipe;

	in_seq = 0;
	out_seq = 0;

	uint64_t supported =
		CEPH_FEATURE_UID |
		CEPH_FEATURE_NOSRCADDR |
		DEPRECATED_CEPH_FEATURE_MONCLOCKCHECK |
		CEPH_FEATURE_PGID64 |
		CEPH_FEATURE_MSG_AUTH;

	accept_sd = *((int *)_arg);

	pipe.recv_len = 0;
	pipe.recv_ofs = 0;
	pipe.accept_sd = accept_sd;
	printf("begin reader \n");
	
	ret = accept(&pipe, &supported);
	if (ret) {
		printf("accept error %d \n", ret);
		goto err_ret;
	}

	printf("begin reader ok\n");
	while (1) {
		printf("reader reading tag...\n");
		if (tcp_read((char*)&tag, 1, &pipe) < 0) {
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
			ret = tcp_read((char*)&t, sizeof(t), &pipe);
			if (ret < 0) {
				printf("reader couldn't read KEEPALIVE2 stamp %d\n", errno);
				goto err_ret;
			} else {
				keepalive_ack_stamp = utime_t(t);
				//connection_state->set_last_keepalive(ceph_clock_now(NULL));
				ret = write_keepalive2(CEPH_MSGR_TAG_KEEPALIVE2_ACK, t, accept_sd);
				if (ret) {
					printf("write keepalive2 error %d\n", ret);
					goto err_ret;
				}
			}
			continue;
		}

		if (tag == CEPH_MSGR_TAG_KEEPALIVE2_ACK) {
			printf("reader got KEEPALIVE_ACK\n");
			ret = tcp_read((char*)&t, sizeof(t), &pipe);
			if (ret < 0) {
				printf("reader couldn't read KEEPALIVE2 stamp %d\n", errno);
				goto err_ret;
			} else {
				//connection_state->set_last_keepalive_ack(utime_t(t));
			}
			continue;
		}

		if (tag == CEPH_MSGR_TAG_ACK) {
			printf("reader got ACK\n");
			ret = tcp_read((char*)&seq, sizeof(seq), &pipe);
			if (ret < 0) {
				printf("reader couldn't read ack seq, %d\n", errno);
				goto err_ret;
			}
		       	//else if (state != STATE_CLOSED) {
				//handle_ack(seq);
			//}
			continue;
		} else if (tag == CEPH_MSGR_TAG_MSG) {
			printf("reader got MSG\n");
      			Message *m = 0;
      			ret = read_message(&m, &supported, &pipe);
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

      			//m->set_connection(connection_state.get());
      			in_seq = m->get_seq();
			write_ack(in_seq, accept_sd);

			printf("get message type: %d", m->get_type());
#if 0
      			//Message *rsp_m = new Message(m->get_type());
			//ret = fetch_msg(m, NULL);
			//if (ret) {
				//goto err_ret;
			//}

			rsp_m->set_seq(++out_seq);
			ret = write_message(rsp_m, accept_sd);
			if (ret) {
				goto err_ret;
			}
#endif
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
	return NULL;
}

void *__server_start(void *_arg)
{
	int ret, socket_fd, listen_port, accept_sd;
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

	ret = ::bind(socket_fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
	if (ret < 0) {
		ret = -errno;
		printf("bind socket error, ret, %d\n", ret);
		goto err_ret;
	}

	ret = ::listen(socket_fd, 128);
	if (ret < 0) {
		ret = -errno;
		dout(0) << "listen socket error, ret " << ret << dendl;
		goto err_ret;
	}

	printf("i am ready for : %d\n", listen_port);
	while (1) {
		entity_addr_t addr;
		socklen_t slen = sizeof(addr.ss_addr());
		accept_sd = ::accept(socket_fd, (sockaddr*)&addr.ss_addr(), &slen);
		if (accept_sd < 0) {
			ret = -errno;
			dout(0) << "listen socket error, ret " << ret << dendl;
			goto err_ret;
		}

		arg = (int *)malloc(sizeof(*arg));
		*arg = accept_sd;
		ret = pthread_create(&th, NULL, __reader, (void *)arg);
		if (ret) {
			dout(0) << "thread create error, ret " << ret << dendl;
			goto err_ret;
		}
	}

	//return 0;
	return NULL;
err_ret:
	//return ret;
	return NULL;
}

int server_start(int listen_port)
{
	int ret;
	int *arg;
	pthread_t th;

	arg = (int *)malloc(sizeof(*arg));
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

	ret = server_start(6789);
	if (ret) {
		goto err_ret;
	}

	/*
	ret = server_start(6790);
	if (ret) {
		goto err_ret;
	}
	*/

	pthread_exit(NULL);

	return 0;
err_ret:
	return ret;
}
