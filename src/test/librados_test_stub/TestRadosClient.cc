// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados_test_stub/TestRadosClient.h"
#include "test/librados_test_stub/TestIoCtxImpl.h"
#include "librados/AioCompletionImpl.h"
#include "include/assert.h"
#include "common/ceph_json.h"
#include "common/Finisher.h"
#include <errno.h>

namespace librados {

class AioFunctionContext : public Context {
public:
  AioFunctionContext(const TestRadosClient::AioFunction &callback,
                     AioCompletionImpl *c)
    : m_callback(callback), m_comp(c)
  {
    m_comp->get();
  }

  virtual void finish(int r) {
    int ret = m_callback();
    if (m_comp != NULL) {
      m_comp->lock.Lock();
      m_comp->ack = true;
      m_comp->safe = true;
      m_comp->rval = ret;
      m_comp->lock.Unlock();

      rados_callback_t cb_complete = m_comp->callback_complete;
      void *cb_complete_arg = m_comp->callback_complete_arg;
      if (cb_complete) {
        cb_complete(m_comp, cb_complete_arg);
      }

      rados_callback_t cb_safe = m_comp->callback_safe;
      void *cb_safe_arg = m_comp->callback_safe_arg;
      if (cb_safe) {
        cb_safe(m_comp, cb_safe_arg);
      }

      m_comp->lock.Lock();
      m_comp->callback_complete = NULL;
      m_comp->callback_safe = NULL;
      m_comp->cond.Signal();
      m_comp->put_unlock();
    }
  }
private:
  TestRadosClient::AioFunction m_callback;
  AioCompletionImpl *m_comp;
};

TestRadosClient::TestRadosClient(CephContext *cct)
  : m_cct(cct->get()),
    m_finisher(new Finisher(m_cct)),
    m_watch_notify(m_cct, m_finisher)
{
  get();
  m_finisher->start();
}

TestRadosClient::~TestRadosClient() {
  flush_aio_operations();
  m_finisher->stop();
  delete m_finisher;

  m_cct->put();
  m_cct = NULL;
}

void TestRadosClient::get() {
  m_refcount.inc();
}

void TestRadosClient::put() {
  if (m_refcount.dec() == 0) {
    shutdown();
    delete this;
  }
}

CephContext *TestRadosClient::cct() {
  return m_cct;
}

uint64_t TestRadosClient::get_instance_id() {
  return 0;
}

int TestRadosClient::connect() {
  return 0;
}

void TestRadosClient::shutdown() {
}

int TestRadosClient::wait_for_latest_osdmap() {
  return 0;
}

int TestRadosClient::mon_command(const std::vector<std::string>& cmd,
                                 const bufferlist &inbl,
                                 bufferlist *outbl, std::string *outs) {
  for (std::vector<std::string>::const_iterator it = cmd.begin();
       it != cmd.end(); ++it) {
    JSONParser parser;
    if (!parser.parse(it->c_str(), it->length())) {
      return -EINVAL;
    }

    JSONObjIter j_it = parser.find("prefix");
    if (j_it.end()) {
      return -EINVAL;
    }

    if ((*j_it)->get_data() == "osd tier add") {
      return 0;
    } else if ((*j_it)->get_data() == "osd tier cache-mode") {
      return 0;
    } else if ((*j_it)->get_data() == "osd tier set-overlay") {
      return 0;
    } else if ((*j_it)->get_data() == "osd tier remove-overlay") {
      return 0;
    } else if ((*j_it)->get_data() == "osd tier remove") {
      return 0;
    }
  }
  return -ENOSYS;
}

void TestRadosClient::add_aio_operation(const AioFunction &aio_function,
                                        AioCompletionImpl *c) {
  AioFunctionContext *ctx = new AioFunctionContext(aio_function, c);
  m_finisher->queue(ctx);
}

void TestRadosClient::flush_aio_operations() {
  m_finisher->wait_for_empty();
}

} // namespace librados
