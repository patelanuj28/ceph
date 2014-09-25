// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 David Zafman <dzafman@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include <string>

#ifndef CEPH_LIBRADOS_LISTOBJECTIMPL_H
#define CEPH_LIBRADOS_LISTOBJECTIMPL_H

namespace librados {
struct ListObjectImpl {
  std::string nspace;
  std::string oid;
  std::string locator;

  ListObjectImpl() {}
  ListObjectImpl(std::string n, std::string o, std::string l):
      nspace(n), oid(o), locator(l) {}

  std::string get_nspace() { return nspace; }
  std::string get_oid() { return oid; }
  std::string get_locator() { return locator; }
};
WRITE_EQ_OPERATORS_3(ListObjectImpl, nspace, oid, locator)
WRITE_CMP_OPERATORS_3(ListObjectImpl, nspace, oid, locator)
inline std::ostream& operator<<(std::ostream& out, const struct ListObjectImpl& lop) {
  out << (lop.nspace.size() ? lop.nspace + "/" : "") << lop.oid
      << (lop.locator.size() ? "@" + lop.locator : "");
  return out;
}
}
#endif
