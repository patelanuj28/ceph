// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/ImageWatcher.h"
#include "librbd/AioCompletion.h"
#include "librbd/ImageCtx.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "include/encoding.h"
#include "include/stringify.h"
#include "common/errno.h"
#include <sstream>
#include <boost/bind.hpp>
#include <boost/function.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ImageWatcher: "

namespace librbd {

static const std::string WATCHER_LOCK_TAG = "internal";
static const std::string WATCHER_LOCK_COOKIE_PREFIX = "auto";

static const uint64_t	NOTIFY_TIMEOUT = 5000;
static const uint8_t	NOTIFY_VERSION = 1;

static const uint8_t	NOTIFY_OP_ANNOUNCE_LOCKED = 0;
static const uint8_t	NOTIFY_OP_RELEASE_LOCK = 1;
static const uint8_t	NOTIFY_OP_REQUEST_LOCK = 2;
static const uint8_t	NOTIFY_OP_HEADER_UPDATE = 3;

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


ImageWatcher::ImageWatcher(ImageCtx &image_ctx)
  : m_image_ctx(image_ctx), m_watch_ctx(*this), m_handle(0), m_lock_owner(false),
    m_finisher(new Finisher(image_ctx.cct)),
    m_lock_request_lock("librbd::ImageWatcher::m_lock_request_lock")
{
  m_finisher->start();
}

ImageWatcher::~ImageWatcher()
{
  m_finisher->stop();
  delete m_finisher;
}

bool ImageWatcher::is_lock_supported() const {
  return ((m_image_ctx.features & RBD_FEATURE_EXCLUSIVE_LOCK) != 0 &&
	  !m_image_ctx.read_only && m_image_ctx.snap_id == CEPH_NOSNAP);
}

bool ImageWatcher::is_lock_owner() const {
  // TODO issue #8903 will address lost notification handling
  // in cases where the lock was broken
  return m_lock_owner;
}

int ImageWatcher::register_watch() {
  ldout(m_image_ctx.cct, 20) << "registering image watcher" << dendl;
  return m_image_ctx.md_ctx.watch2(m_image_ctx.header_oid, &m_handle,
				   &m_watch_ctx);
}

int ImageWatcher::unregister_watch() {
  ldout(m_image_ctx.cct, 20)  << "unregistering image watcher" << dendl;
  cancel_aio_requests(-ESHUTDOWN);

  return m_image_ctx.md_ctx.unwatch(m_image_ctx.header_oid, m_handle);;
}

int ImageWatcher::try_lock() {
  assert(m_lock_owner == false);
  assert(m_image_ctx.leader_lock.is_wlocked());

  int r = lock();
  if (r != -EBUSY) {
    return r;
  }

  // determine if the current lock holder is still alive
  entity_name_t locker;
  std::string locker_cookie;
  std::string locker_address;
  uint64_t locker_handle;
  r = get_lock_owner_info(&locker, &locker_cookie, &locker_address,
			  &locker_handle);
  if (r < 0 || locker_cookie.empty() || locker_address.empty()) {
    return r;
  }

  std::list<obj_watch_t> watchers;
  r = m_image_ctx.md_ctx.list_watchers(m_image_ctx.header_oid, &watchers);
  if (r < 0) {
    return r;
  }

  for (std::list<obj_watch_t>::iterator iter = watchers.begin();
       iter != watchers.end(); ++iter) {
    if ((strncmp(locker_address.c_str(), iter->addr, sizeof(iter->addr)) == 0) &&
	(locker_handle == iter->cookie)) {
      return 0;
    }
  }

  ldout(m_image_ctx.cct, 1) << "breaking exclusive lock: " << locker << dendl;
  r = rados::cls::lock::break_lock(&m_image_ctx.md_ctx, m_image_ctx.header_oid,
				   RBD_LOCK_NAME, locker_cookie, locker);
  if (r < 0 && r != -ENOENT) {
    return r;
  }
  return -EAGAIN;
}

int ImageWatcher::request_lock(
    const boost::function<int(AioCompletion*)>& restart_op, AioCompletion* c) {
  assert(m_lock_owner == false);
  assert(m_image_ctx.leader_lock.is_locked());

  {
    Mutex::Locker l(m_lock_request_lock);
    bool request_pending = !m_lock_request_restarts.empty();
    m_lock_request_restarts.push_back(std::make_pair(restart_op, c));
    if (request_pending) {
      return 0;
    }
  }

  // run notify request in finisher to avoid blocking aio path
  FunctionContext *ctx = new FunctionContext(
    boost::bind(&ImageWatcher::notify_request_lock, this));
  m_finisher->queue(ctx);
  ldout(m_image_ctx.cct, 5) << "requesting exclusive lock" << dendl;
  return 0;
}

void ImageWatcher::finalize_request_lock() {
  {
    RWLock::WLocker l(m_image_ctx.leader_lock);
    int r = try_lock();
    if (r < 0 && r != -EAGAIN) {
      ldout(m_image_ctx.cct, 5) << "failed to request exclusive lock:"
				<< cpp_strerror(r) << dendl;
      cancel_aio_requests(-EROFS);
      return;
    }

    if (is_lock_owner()) {
      ldout(m_image_ctx.cct, 5) << "successfully requested exclusive lock"
				<< dendl;
    } else {
      ldout(m_image_ctx.cct, 5) << "unable to acquire exclusive lock, retrying"
				<< dendl;
    }
  }

  retry_aio_requests();
}

int ImageWatcher::get_lock_owner_info(entity_name_t *locker, std::string *cookie,
				      std::string *address, uint64_t *handle) {
  std::map<rados::cls::lock::locker_id_t,
	   rados::cls::lock::locker_info_t> lockers;
  ClsLockType lock_type;
  std::string lock_tag;
  int r = rados::cls::lock::get_lock_info(&m_image_ctx.md_ctx,
					  m_image_ctx.header_oid,
					  RBD_LOCK_NAME, &lockers, &lock_type,
					  &lock_tag);
  if (r < 0 || lockers.empty() || lock_tag != WATCHER_LOCK_TAG) {
    return r;
  }

  if (lock_type == LOCK_SHARED) {
    lderr(m_image_ctx.cct) << "invalid lock type detected" << dendl;
    return -EINVAL;
  }

  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t>::iterator iter = lockers.begin();
  if (!decode_lock_cookie(iter->first.cookie, handle)) {
    return 0;
  }

  *locker = iter->first.locker;
  *cookie = iter->first.cookie;
  *address = stringify(iter->second.addr);
  ldout(m_image_ctx.cct, 10) << "retrieved exclusive locker: " << *locker
			     << "@" << *address << dendl;
  return 0;
}

int ImageWatcher::lock() {
  int r = rados::cls::lock::lock(&m_image_ctx.md_ctx, m_image_ctx.header_oid,
				 RBD_LOCK_NAME, LOCK_EXCLUSIVE,
				 encode_lock_cookie(), WATCHER_LOCK_TAG, "",
				 utime_t(), 0);
  if (r < 0) {
    return r;
  }

  ldout(m_image_ctx.cct, 20) << "acquired exclusive lock" << dendl;
  m_lock_owner = true;

  bufferlist bl;
  ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, bl);
  ::encode(NOTIFY_OP_ANNOUNCE_LOCKED, bl);
  ENCODE_FINISH(bl);

  m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT, NULL);
  return 0;
}

int ImageWatcher::unlock()
{
  assert(m_image_ctx.leader_lock.is_wlocked());
  if (!m_lock_owner) {
    return 0;
  }

  ldout(m_image_ctx.cct, 20) << "releasing exclusive lock" << dendl;
  m_lock_owner = false;
  int r = rados::cls::lock::unlock(&m_image_ctx.md_ctx, m_image_ctx.header_oid,
				   RBD_LOCK_NAME, encode_lock_cookie());
  if (r < 0 && r != -ENOENT) {
    lderr(m_image_ctx.cct) << "failed to release exclusive lock: "
			   << cpp_strerror(r) << dendl;
    return r;
  }

  bufferlist bl;
  ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, bl);
  ::encode(NOTIFY_OP_RELEASE_LOCK, bl);
  ENCODE_FINISH(bl);
  m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT, NULL);
  return 0;
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

std::string ImageWatcher::encode_lock_cookie() const {
  std::ostringstream ss;
  ss << WATCHER_LOCK_COOKIE_PREFIX << " " << m_handle;
  return ss.str();
}

bool ImageWatcher::decode_lock_cookie(const std::string &tag,
				      uint64_t *handle) {
  std::string prefix;
  std::istringstream ss(tag);
  if (!(ss >> prefix >> *handle) || prefix != WATCHER_LOCK_COOKIE_PREFIX) {
    return false;
  }
  return true;
}

void ImageWatcher::retry_aio_requests() {
  std::vector<AioRequest> lock_request_restarts;
  {
    Mutex::Locker l(m_lock_request_lock);
    lock_request_restarts.swap(m_lock_request_restarts);
  }

  for (std::vector<AioRequest>::iterator iter = lock_request_restarts.begin();
       iter != lock_request_restarts.end(); ++iter) {
    iter->first(iter->second);
  }
}

void ImageWatcher::cancel_aio_requests(int result) {
  std::vector<AioRequest> lock_request_restarts;
  {
    Mutex::Locker l(m_lock_request_lock);
    lock_request_restarts.swap(m_lock_request_restarts);
  }

  for (std::vector<AioRequest>::iterator iter = lock_request_restarts.begin();
       iter != lock_request_restarts.end(); ++iter) {
    AioCompletion *c = iter->second;
    c->get();
    c->lock.Lock();
    c->rval = result;
    c->lock.Unlock();
    c->finish_adding_requests(m_image_ctx.cct);
    c->put();
  }
}

int ImageWatcher::decode_response_code(bufferlist &bl) {
  int r;
  bufferlist::iterator iter = bl.begin();
  DECODE_START(NOTIFY_VERSION, iter);
  ::decode(r, iter);
  DECODE_FINISH(iter);
  return r;
}

void ImageWatcher::notify_request_lock()
{
  bufferlist bl;
  ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, bl);
  ::encode(NOTIFY_OP_REQUEST_LOCK, bl);
  ENCODE_FINISH(bl);

  bufferlist response;
  int r = notify_lock_owner(bl, response);
  if (r == -ETIMEDOUT) {
    ldout(m_image_ctx.cct, 5) << "timed out requesting lock: retrying" << dendl;
    retry_aio_requests();
  } else if (r < 0) {
    lderr(m_image_ctx.cct) << "error requesting lock: " << cpp_strerror(r)
			   << dendl;
    cancel_aio_requests(-EROFS);
  }
}

int ImageWatcher::notify_lock_owner(bufferlist &bl, bufferlist& response) {
  bufferlist response_bl;
  int r = m_image_ctx.md_ctx.notify2(m_image_ctx.header_oid, bl, NOTIFY_TIMEOUT,
				     &response_bl);
  if (r < 0 && r != -ETIMEDOUT) {
    lderr(m_image_ctx.cct) << "lock owner notification failed: "
			   << cpp_strerror(r) << dendl;
    return r;
  }

  std::multimap<uint64_t, bufferlist> responses;
  if (response_bl.length() > 0) {
    try {
      bufferlist::iterator iter = response_bl.begin();
      ::decode(responses, iter);
    } catch (const buffer::error &err) {
      lderr(m_image_ctx.cct) << "failed to decode response" << dendl;
      return -EINVAL;
    }
  }

  bool lock_owner_responded = false;
  for (std::multimap<uint64_t, bufferlist>::iterator i = responses.begin();
       i != responses.end(); ++i) {
    if (i->second.length() > 0) {
      if (lock_owner_responded) {
	lderr(m_image_ctx.cct) << "duplicate lock owners detected" << dendl;
	return -EIO;
      }
      lock_owner_responded = true;
      response.claim(i->second);
    }
  }

  if (!lock_owner_responded) {
    lderr(m_image_ctx.cct) << "no lock owners detected" << dendl;
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

void ImageWatcher::handle_announce_locked() {
}

void ImageWatcher::handle_release_lock() {
  ldout(m_image_ctx.cct, 20) << "exclusive lock released" << dendl;

  Mutex::Locker l(m_lock_request_lock);
  if (!m_lock_request_restarts.empty()) {
    ldout(m_image_ctx.cct, 20) << "queuing lock request" << dendl;
    FunctionContext *ctx = new FunctionContext(
      boost::bind(&ImageWatcher::finalize_request_lock, this));
    m_finisher->queue(ctx);
  }
}

void ImageWatcher::handle_request_lock(bufferlist *out) {
  if (is_lock_owner()) {
    // need to send something back so the client can detect a missing leader
    ENCODE_START(NOTIFY_VERSION, NOTIFY_VERSION, *out);
    ::encode(0, *out);
    ENCODE_FINISH(*out);

    ldout(m_image_ctx.cct, 5) << "exclusive lock requested, releasing" << dendl;
    FunctionContext *ctx = new FunctionContext(
      boost::bind(&ImageWatcher::release_lock, this));
    m_finisher->queue(ctx);
  }
}

void ImageWatcher::handle_unknown_op(bufferlist *out) {
  if (is_lock_owner()) {
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
    case NOTIFY_OP_ANNOUNCE_LOCKED:
      acknowledge_notify(notify_id, handle, out);
      handle_announce_locked();
      break;
    case NOTIFY_OP_RELEASE_LOCK:
      acknowledge_notify(notify_id, handle, out);
      handle_release_lock();
      break;
    case NOTIFY_OP_HEADER_UPDATE:
      acknowledge_notify(notify_id, handle, out);
      handle_header_update();
      break;

    // lock owner-only ops
    case NOTIFY_OP_REQUEST_LOCK:
      handle_request_lock(&out);
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
