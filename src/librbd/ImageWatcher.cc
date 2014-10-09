// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/ImageWatcher.h"
#include "librbd/AioCompletion.h"
#include "librbd/ImageCtx.h"
#include "cls/lock/cls_lock_client.h"
#include "include/encoding.h"
#include "common/errno.h"
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/scope_exit.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ImageWatcher: "

static void decode(librbd::RemoteAsyncRequest &request,
		   bufferlist::iterator &iter) {
  ::decode(request.gid, iter);
  ::decode(request.handle, iter);
  ::decode(request.request_id, iter);
}

static void encode(const librbd::RemoteAsyncRequest &request, bufferlist &bl) {
  ::encode(request.gid, bl);
  ::encode(request.handle, bl);
  ::encode(request.request_id, bl);
}

static std::ostream &operator<<(std::ostream &out,
				const librbd::RemoteAsyncRequest &request) {
  out << "[" << request.gid << "," << request.handle << ","
      << request.request_id << "]";
  return out;
}

namespace librbd {

static const std::string LEADER_LOCK_TAG = "leader";

static const uint64_t	NOTIFY_TIMEOUT = 5000;
static const uint8_t	NOTIFY_VERSION = 1;

static const uint8_t	NOTIFY_OP_ANNOUNCE_LEADER = 0;
static const uint8_t	NOTIFY_OP_RELEASE_LEADERSHIP = 1;
static const uint8_t	NOTIFY_OP_REQUEST_LEADERSHIP = 2;
static const uint8_t	NOTIFY_OP_HEADER_UPDATE = 3;
static const uint8_t	NOTIFY_OP_ASYNC_PROGRESS = 4;
static const uint8_t	NOTIFY_OP_ASYNC_COMPLETE = 5;
static const uint8_t	NOTIFY_OP_FLATTEN = 6;
static const uint8_t	NOTIFY_OP_RESIZE = 7;
static const uint8_t	NOTIFY_OP_SNAP_CREATE = 8;

class FunctionContext : public Context {
public:
  FunctionContext(const boost::function<void()> &callback)
    : m_callback(callback)
  {
  }

  virtual void finish(int r) {
    m_callback();
  }
private:
  boost::function<void()> m_callback;
};

class RemoteProgressContext : public ProgressContext {
public:
  RemoteProgressContext(ImageWatcher &image_watcher, Finisher &finisher,
			const RemoteAsyncRequest &remote_async_request)
    : m_image_watcher(image_watcher), m_finisher(finisher),
      m_remote_async_request(remote_async_request)
  {
  }

  virtual int update_progress(uint64_t offset, uint64_t total) {
    // TODO: JD throttle notify updates(?)
    FunctionContext *ctx = new FunctionContext(
      boost::bind(&ImageWatcher::notify_async_progress,
		  &m_image_watcher, m_remote_async_request, offset, total));
    m_finisher.queue(ctx);
    return 0;
  }

private:
  ImageWatcher &m_image_watcher;
  Finisher &m_finisher;
  RemoteAsyncRequest m_remote_async_request;
};

class RemoteContext : public Context {
public:
  RemoteContext(ImageWatcher &image_watcher, Finisher &finisher,
		const RemoteAsyncRequest &remote_async_request,
		RemoteProgressContext *prog_ctx)
    : m_image_watcher(image_watcher), m_finisher(finisher),
      m_remote_async_request(remote_async_request), m_prog_ctx(prog_ctx)
  {
  }

  ~RemoteContext() {
    delete m_prog_ctx;
  }

  virtual void finish(int r) {
    FunctionContext *ctx = new FunctionContext(
      boost::bind(&ImageWatcher::notify_async_complete,
		  &m_image_watcher, m_remote_async_request, r));
    m_finisher.queue(ctx);
  }

private:
  ImageWatcher &m_image_watcher;
  Finisher &m_finisher;
  RemoteAsyncRequest m_remote_async_request;
  RemoteProgressContext *m_prog_ctx;
};

ImageWatcher::ImageWatcher(ImageCtx &image_ctx)
  : m_image_ctx(image_ctx), m_watch_ctx(*this), m_handle(0), m_leader(false),
    m_finisher(new Finisher(image_ctx.cct)),
    m_async_request_lock("librbd::ImageWatcher::m_async_request_lock"),
    m_async_request_id(0),
    m_leader_request_lock("librbd::ImageWatcher::m_leader_request_lock")
{
  m_finisher->start();
}

ImageWatcher::~ImageWatcher()
{
  m_finisher->stop();
  delete m_finisher;
}

bool ImageWatcher::lock_supported() const {
  return ((m_image_ctx.features & RBD_FEATURE_EXCLUSIVE_LOCK) != 0 &&
	  !m_image_ctx.read_only && m_image_ctx.snap_id == CEPH_NOSNAP);
}

int ImageWatcher::register_watch() {
  ldout(m_image_ctx.cct, 20) << "registering image watcher" << dendl;
  return m_image_ctx.md_ctx.watch2(m_image_ctx.header_oid, &m_handle,
				   &m_watch_ctx);
}

int ImageWatcher::unregister_watch() {
  ldout(m_image_ctx.cct, 20)  << "unregistering image watcher" << dendl;
  cancel_aio_requests(-ESHUTDOWN);
  cancel_async_requests(-ESHUTDOWN);
  return m_image_ctx.md_ctx.unwatch(m_image_ctx.header_oid, m_handle);;
}

int ImageWatcher::try_lock() {
  int r = lock();
  if (r < 0 && r != -EBUSY) {
    return r;
  }
  return 0;
}

int ImageWatcher::request_lock(
    const boost::function<int(AioCompletion*)>& restart_op, AioCompletion* c) {
  assert(m_leader == false);
  assert(m_image_ctx.leader_lock.is_locked());

  {
    Mutex::Locker l(m_leader_request_lock);
    bool request_pending = !m_leader_request_restarts.empty();
    m_leader_request_restarts.push_back(std::make_pair(restart_op, c));
    if (request_pending) {
      return 0;
    }
  }

  // run notify request in finisher to avoid blocking aio path
  FunctionContext *ctx = new FunctionContext(
    boost::bind(&ImageWatcher::notify_request_leadership, this));
  m_finisher->queue(ctx);
  ldout(m_image_ctx.cct, 5) << "requesting exclusive lock" << dendl;
  return 0;
}

void ImageWatcher::finalize_request_lock() {
  {
    RWLock::WLocker l(m_image_ctx.leader_lock);
    int r = try_lock();
    if (r < 0) {
      // lost leadership to another client
      ldout(m_image_ctx.cct, 5) << "failed to request exclusive lock (confict)"
				<< dendl;
      cancel_aio_requests(-EROFS);
      return;
    }
  }

  ldout(m_image_ctx.cct, 5) << "successfully requested exclusive lock" << dendl;
  Mutex::Locker l(m_leader_request_lock);
  for (std::vector<AioRequest>::iterator iter = m_leader_request_restarts.begin();
       iter != m_leader_request_restarts.end(); ++iter) {
    iter->first(iter->second);
  }
  m_leader_request_restarts.clear();
}

int ImageWatcher::lock() {
  assert(m_leader == false);
  assert(m_image_ctx.leader_lock.is_wlocked());
  int r = rados::cls::lock::lock(&m_image_ctx.md_ctx, m_image_ctx.header_oid,
				 RBD_EXCLUSIVE_LOCK_NAME, LOCK_EXCLUSIVE,
				 LEADER_LOCK_TAG, "", "", utime_t(), 0);
  if (r < 0) {
    return r;
  }

  ldout(m_image_ctx.cct, 20) << "acquired exclusive lock" << dendl;
  m_leader = true;

  bufferlist bl;
  ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, bl);
  ::encode(NOTIFY_OP_ANNOUNCE_LEADER, bl);
  ENCODE_FINISH(bl);

  m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT, NULL);
  return 0;
}

int ImageWatcher::unlock()
{
  assert(m_image_ctx.leader_lock.is_wlocked());
  if (!m_leader) {
    return 0;
  }

  ldout(m_image_ctx.cct, 20) << "releasing exclusive lock" << dendl;
  m_leader = false;
  int r = rados::cls::lock::unlock(&m_image_ctx.md_ctx, m_image_ctx.header_oid,
				   RBD_EXCLUSIVE_LOCK_NAME, LEADER_LOCK_TAG);

  bufferlist bl;
  ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, bl);
  ::encode(NOTIFY_OP_RELEASE_LEADERSHIP, bl);
  ENCODE_FINISH(bl);

  m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT, NULL);
  return r;
}

void ImageWatcher::release_lock()
{
  RWLock::WLocker l(m_image_ctx.leader_lock);
  {
    RWLock::WLocker l2(m_image_ctx.md_lock);
    m_image_ctx.invalidate_cache();
  }
  m_image_ctx.data_ctx.aio_flush();

  unlock();
}

int ImageWatcher::notify_async_progress(const RemoteAsyncRequest &request,
					uint64_t offset, uint64_t total) {
  ldout(m_image_ctx.cct, 20) << "remote async request progress: "
			     << request << " @ " << offset
			     << "/" << total << dendl;

  bufferlist bl;
  ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, bl);
  ::encode(NOTIFY_OP_ASYNC_PROGRESS, bl);
  ::encode(request, bl);
  ::encode(offset, bl);
  ::encode(total, bl);
  ENCODE_FINISH(bl);

  m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT, NULL);
  return 0;
}

int ImageWatcher::notify_async_complete(const RemoteAsyncRequest &request,
					int r) {
  ldout(m_image_ctx.cct, 20) << "remote async request finished: "
			     << request << " = " << r << dendl;

  bufferlist bl;
  ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, bl);
  ::encode(NOTIFY_OP_ASYNC_COMPLETE, bl);
  ::encode(request, bl);
  ::encode(r, bl);
  ENCODE_FINISH(bl);

  librbd::notify_change(m_image_ctx.md_ctx, m_image_ctx.header_oid,
			&m_image_ctx);
  m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT, NULL);
  return 0;
}

int ImageWatcher::notify_flatten(ProgressContext &prog_ctx) {
  bufferlist bl;
  uint64_t async_request_id;
  ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, bl);
  ::encode(NOTIFY_OP_FLATTEN, bl);
  async_request_id = encode_async_request(bl);
  ENCODE_FINISH(bl);

  return notify_async_request(async_request_id, bl, prog_ctx);
}

int ImageWatcher::notify_resize(uint64_t size, ProgressContext &prog_ctx) {
  bufferlist bl;
  uint64_t async_request_id;
  ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, bl);
  ::encode(NOTIFY_OP_RESIZE, bl);
  ::encode(size, bl);
  async_request_id = encode_async_request(bl);
  ENCODE_FINISH(bl);

  return notify_async_request(async_request_id, bl, prog_ctx);
}

int ImageWatcher::notify_snap_create(const std::string &snap_name) {
  bufferlist bl;
  ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, bl);
  ::encode(NOTIFY_OP_SNAP_CREATE, bl);
  ::encode(snap_name, bl);
  ENCODE_FINISH(bl);

  bufferlist response;
  int r = notify_leader(bl, response);
  if (r < 0) {
    return r;
  }
  return decode_response_code(response);
}

void ImageWatcher::notify_header_update(librados::IoCtx &io_ctx,
				        const std::string &oid)
{
  // supports legacy (empty buffer) clients
  bufferlist bl;
  ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, bl);
  ::encode(NOTIFY_OP_HEADER_UPDATE, bl);
  ENCODE_FINISH(bl);

  io_ctx.notify2(oid, bl, NOTIFY_TIMEOUT, NULL);
}

void ImageWatcher::cancel_aio_requests(int result) {
  Mutex::Locker l(m_leader_request_lock);
  for (std::vector<AioRequest>::iterator iter = m_leader_request_restarts.begin();
       iter != m_leader_request_restarts.end(); ++iter) {
    AioCompletion *c = iter->second;
    c->get();
    c->lock.Lock();
    c->rval = result;
    c->lock.Unlock();
    c->finish_adding_requests(m_image_ctx.cct);
    c->put();
  }
  m_leader_request_restarts.clear();
}

void ImageWatcher::cancel_async_requests(int result) {
  RWLock::WLocker l(m_async_request_lock);
  for (std::map<uint64_t, AsyncRequest>::iterator iter = m_async_requests.begin();
       iter != m_async_requests.end(); ++iter) {
    iter->second.first->complete(result);
  }
  m_async_requests.clear();
}

uint64_t ImageWatcher::encode_async_request(bufferlist &bl) {
  RWLock::WLocker l(m_async_request_lock);
  ++m_async_request_id;

  RemoteAsyncRequest request(m_image_ctx.md_ctx.get_instance_id(),
			     m_handle, m_async_request_id);
  ::encode(request, bl);

  ldout(m_image_ctx.cct, 20) << "async request: " << request << dendl;
  return m_async_request_id;
}

int ImageWatcher::decode_response_code(bufferlist &bl) {
  int r;
  bufferlist::iterator iter = bl.begin();
  DECODE_START(NOTIFY_VERSION, iter);
  ::decode(r, iter);
  DECODE_FINISH(iter);
  return r;
}

int ImageWatcher::notify_async_request(uint64_t async_request_id,
				       bufferlist &in,
				       ProgressContext& prog_ctx) {
  Mutex my_lock("librbd::ImageWatcher::notify_async_request::my_lock");
  Cond cond;
  bool done = false;
  int r;
  Context *ctx = new C_SafeCond(&my_lock, &cond, &done, &r);

  {
    RWLock::WLocker l(m_async_request_lock);
    m_async_requests[async_request_id] = AsyncRequest(ctx, &prog_ctx);
  }

  BOOST_SCOPE_EXIT( (ctx)(async_request_id)(&m_async_requests)
		    (&m_async_request_lock)(&done) ) {
    RWLock::WLocker l(m_async_request_lock);
    m_async_requests.erase(async_request_id);
    if (!done) {
      delete ctx;
    }
  } BOOST_SCOPE_EXIT_END

  bufferlist response;
  r = notify_leader(in, response);
  if (r < 0) {
    return r;
  }

  my_lock.Lock();
  while (!done) {
    cond.Wait(my_lock);
  }
  my_lock.Unlock();
  return r;
}

void ImageWatcher::notify_request_leadership()
{
  bufferlist bl;
  ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, bl);
  ::encode(NOTIFY_OP_REQUEST_LEADERSHIP, bl);
  ENCODE_FINISH(bl);

  int r = m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT,
				     NULL);
  if (r < 0) {
    lderr(m_image_ctx.cct) << "error requesting leadership: " << cpp_strerror(r)
			   << dendl;
    cancel_aio_requests(-EROFS);
  }
}

int ImageWatcher::notify_leader(bufferlist &bl, bufferlist& response) {
  bufferlist response_bl;
  int r = m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT,
				     &response_bl);
  if (r < 0) {
    lderr(m_image_ctx.cct) << "timed out sending notification: "
			   << cpp_strerror(r) << dendl;
    return r;
  }

  bufferlist::iterator iter = response_bl.begin();
  std::multimap<uint64_t, bufferlist> responses;
  ::decode(responses, iter);

  bool leader_responded = false;
  for (std::multimap<uint64_t, bufferlist>::iterator i = responses.begin();
       i != responses.end(); ++i) {
    if (i->second.length() > 0) {
      if (leader_responded) {
	lderr(m_image_ctx.cct) << "duplicate leaders detected" << dendl;
	return -EIO;
      }
      leader_responded = true;
      response.claim(i->second);
    }
  }

  if (!leader_responded) {
    lderr(m_image_ctx.cct) << "no leaders detected" << dendl;
    return -ETIMEDOUT;
  }
  return 0;
}

void ImageWatcher::handle_header_update() {
  ldout(m_image_ctx.cct, 1) << "image header updated" << dendl;

  Mutex::Locker lictx(m_image_ctx.refresh_lock);
  ++m_image_ctx.refresh_seq;
  m_image_ctx.perfcounter->inc(l_librbd_notify);
}

void ImageWatcher::handle_announce_leader() {
  FunctionContext *ctx = new FunctionContext(
    boost::bind(&ImageWatcher::cancel_async_requests, this, -ERESTART));
  m_finisher->queue(ctx);
}

void ImageWatcher::handle_release_leadership() {
  ldout(m_image_ctx.cct, 20) << "exclusive lock released by leader" << dendl;

  Mutex::Locker l(m_leader_request_lock);
  if (!m_leader_request_restarts.empty()) {
    ldout(m_image_ctx.cct, 20) << "queuing lock request" << dendl;
    FunctionContext *ctx = new FunctionContext(
      boost::bind(&ImageWatcher::finalize_request_lock, this));
    m_finisher->queue(ctx);
  }
}

void ImageWatcher::handle_request_leadership() {
  if (is_leader()) {
    ldout(m_image_ctx.cct, 5) << "exclusive lock requested, releasing" << dendl;
    FunctionContext *ctx = new FunctionContext(
      boost::bind(&ImageWatcher::release_lock, this));
    m_finisher->queue(ctx);
  }
}

void ImageWatcher::handle_async_progress(bufferlist::iterator iter) {
  RemoteAsyncRequest request;
  ::decode(request, iter);

  uint64_t offset;
  uint64_t total;
  ::decode(offset, iter);
  ::decode(total, iter);
  if (request.gid == m_image_ctx.md_ctx.get_instance_id() &&
      request.handle == m_handle) {
    RWLock::RLocker l(m_async_request_lock);
    std::map<uint64_t, AsyncRequest>::iterator iter =
      m_async_requests.find(request.request_id);
    if (iter != m_async_requests.end()) {
      ldout(m_image_ctx.cct, 20) << "request progress: "
				 << request << " @ " << offset
				 << "/" << total << dendl;
      iter->second.second->update_progress(offset, total);
    }
  }
}

void ImageWatcher::handle_async_complete(bufferlist::iterator iter) {
  RemoteAsyncRequest request;
  ::decode(request, iter);

  int r;
  ::decode(r, iter);
  if (request.gid == m_image_ctx.md_ctx.get_instance_id() &&
      request.handle == m_handle) {
    Context *ctx = NULL;
    {
      RWLock::RLocker l(m_async_request_lock);
      std::map<uint64_t, AsyncRequest>::iterator iter =
        m_async_requests.find(request.request_id);
      if (iter != m_async_requests.end()) {
	ctx = iter->second.first;
      }
    }
    if (ctx != NULL) {
      ldout(m_image_ctx.cct, 20) << "request finished: "
                                 << request << " = " << r << dendl;
      ctx->complete(r);
    }
  }
}

void ImageWatcher::handle_flatten(bufferlist::iterator iter, bufferlist *out) {
  if (is_leader()) {
    RemoteAsyncRequest request;
    ::decode(request, iter);

    RemoteProgressContext *prog_ctx =
      new RemoteProgressContext(*this, *m_finisher, request);
    RemoteContext *ctx = new RemoteContext(*this, *m_finisher, request,
					   prog_ctx);

    ldout(m_image_ctx.cct, 20) << "remote flatten request: " << request << dendl;

    int r = librbd::async_flatten(&m_image_ctx, ctx, *prog_ctx);
    if (r < 0) {
      delete ctx;
    }

    ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, *out);
    ::encode(r, *out);
    ENCODE_FINISH(*out);
  }
}

void ImageWatcher::handle_resize(bufferlist::iterator iter, bufferlist *out) {
  if (is_leader()) {
    uint64_t size;
    ::decode(size, iter);

    RemoteAsyncRequest request;
    ::decode(request, iter);

    RemoteProgressContext *prog_ctx =
      new RemoteProgressContext(*this, *m_finisher, request);
    RemoteContext *ctx = new RemoteContext(*this, *m_finisher, request,
					   prog_ctx);

    ldout(m_image_ctx.cct, 20) << "remote resize request: " << request
			       << " " << size << dendl;

    int r = librbd::async_resize(&m_image_ctx, ctx, size, *prog_ctx);
    if (r < 0) {
      delete ctx;
    }

    ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, *out);
    ::encode(r, *out);
    ENCODE_FINISH(*out);
  }
}

void ImageWatcher::handle_snap_create(bufferlist::iterator iter, bufferlist *out) {
  if (is_leader()) {
    std::string snap_name;
    ::decode(snap_name, iter);

    ldout(m_image_ctx.cct, 20) << "remote snap_create request: " << snap_name << dendl;

    int r = librbd::snap_create(&m_image_ctx, snap_name.c_str());
    ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, *out);
    ::encode(r, *out);
    ENCODE_FINISH(*out);
  }
}

void ImageWatcher::handle_unknown_op(bufferlist *out) {
  if (is_leader()) {
    ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, *out);
    ::encode(-EOPNOTSUPP, *out);
    ENCODE_FINISH(*out);
  }
}

void ImageWatcher::handle_notify(uint64_t notify_id, uint64_t handle,
				 bufferlist &bl) {
  if (bl.length() == 0) {
    // legacy notification for header updates
    bufferlist out;
    acknowledge_notify(notify_id, handle, out);
    handle_header_update();
    return;
  }

  bufferlist::iterator iter = bl.begin();
  try {
    DECODE_START(NOTIFY_VERSION, iter);
    uint8_t op;
    ::decode(op, iter);

    bufferlist out;
    switch (op) {
    // client ops
    case NOTIFY_OP_ANNOUNCE_LEADER:
      acknowledge_notify(notify_id, handle, out);
      handle_announce_leader();
      break;
    case NOTIFY_OP_RELEASE_LEADERSHIP:
      acknowledge_notify(notify_id, handle, out);
      handle_release_leadership();
      break;
    case NOTIFY_OP_HEADER_UPDATE:
      acknowledge_notify(notify_id, handle, out);
      handle_header_update();
      break;
    case NOTIFY_OP_ASYNC_PROGRESS:
      acknowledge_notify(notify_id, handle, out);
      handle_async_progress(iter);
      break;
    case NOTIFY_OP_ASYNC_COMPLETE:
      acknowledge_notify(notify_id, handle, out);
      handle_async_complete(iter);
      break;

    // leader-only ops
    case NOTIFY_OP_REQUEST_LEADERSHIP:
      acknowledge_notify(notify_id, handle, out);
      handle_request_leadership();
      break;
    case NOTIFY_OP_FLATTEN:
      handle_flatten(iter, &out);
      acknowledge_notify(notify_id, handle, out);
      break;
    case NOTIFY_OP_RESIZE:
      handle_resize(iter, &out);
      acknowledge_notify(notify_id, handle, out);
      break;
    case NOTIFY_OP_SNAP_CREATE:
      handle_snap_create(iter, &out);
      acknowledge_notify(notify_id, handle, out);
      break;

    default:
      handle_unknown_op(&out);
      acknowledge_notify(notify_id, handle, out);
      break;
    }
    DECODE_FINISH(iter);
  } catch (const buffer::error &err) {
    lderr(m_image_ctx.cct) << "error decoding image notification" << dendl;
  }
}

void ImageWatcher::acknowledge_notify(uint64_t notify_id, uint64_t handle,
				      bufferlist &out) {
  m_image_ctx.md_ctx.notify_ack(m_image_ctx.header_oid, notify_id, handle, out);
}

void ImageWatcher::WatchCtx::handle_notify(uint64_t notify_id,
	         uint64_t handle, uint64_t notifier_id,
	         bufferlist& bl) {
  image_watcher.handle_notify(notify_id, handle, bl);
}

}
