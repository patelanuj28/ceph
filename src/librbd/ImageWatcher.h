// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_IMAGE_WATCHER_H
#define CEPH_LIBRBD_IMAGE_WATCHER_H

#include "common/Mutex.h"
#include "common/RWLock.h"
#include "include/rados/librados.hpp"
#include <string>
#include <utility>
#include <vector>
#include <boost/function.hpp>
#include "include/assert.h"

class entity_name_t;
class Context;
class Finisher;

namespace librbd {

  class AioCompletion;
  class ImageCtx;

  class ImageWatcher {
  public:

    ImageWatcher(ImageCtx& image_ctx);
    ~ImageWatcher();

    bool is_lock_supported() const;
    bool is_lock_owner() const;

    int register_watch();
    int unregister_watch();

    int try_lock();
    int request_lock(const boost::function<int(AioCompletion*)>& restart_op,
		     AioCompletion* c);
    int unlock();

    static void notify_header_update(librados::IoCtx &io_ctx,
				     const std::string &oid);

  private:

    typedef std::pair<boost::function<int(AioCompletion *)>,
		      AioCompletion *> AioRequest;

    struct WatchCtx : public librados::WatchCtx2 {
      ImageWatcher &image_watcher;

      WatchCtx(ImageWatcher &parent) : image_watcher(parent) {}

      virtual void handle_notify(uint64_t notify_id,
                                 uint64_t handle,
				 uint64_t notifier_id,
                                 bufferlist& bl);
    };

    ImageCtx &m_image_ctx;

    WatchCtx m_watch_ctx;
    uint64_t m_handle;

    bool m_lock_owner;

    Finisher *m_finisher;

    Mutex m_lock_request_lock;
    std::vector<AioRequest> m_lock_request_restarts;

    std::string encode_lock_cookie() const;
    static bool decode_lock_cookie(const std::string &cookie, uint64_t *handle);

    int get_lock_owner_info(entity_name_t *locker, std::string *cookie,
			    std::string *address, uint64_t *handle);
    int lock();
    void release_lock();
    void finalize_request_lock();

    void retry_aio_requests();
    void cancel_aio_requests(int result);
    static int decode_response_code(bufferlist &bl);

    void notify_request_lock();
    int notify_lock_owner(bufferlist &bl, bufferlist &response);

    void handle_header_update();
    void handle_announce_locked();
    void handle_release_lock();
    void handle_request_lock(bufferlist *out);
    void handle_unknown_op(bufferlist *out);
    void handle_notify(uint64_t notify_id, uint64_t handle, bufferlist &bl);
    void acknowledge_notify(uint64_t notify_id, uint64_t handle,
			    bufferlist &out);
  };

} // namespace librbd

#endif // CEPH_LIBRBD_IMAGE_WATCHER_H
