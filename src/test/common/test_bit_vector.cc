// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * LGPL2.1 (see COPYING-LGPL2.1) or later
 */

#include <gtest/gtest.h>
#include <cmath>
#include "common/bit_vector.hpp"

using namespace ceph;

template <uint8_t _bit_count>
class TestParams {
public:
  static const uint8_t BIT_COUNT = _bit_count;
};

template <typename T>
class BitVectorTest : public ::testing::Test {
public:
  typedef BitVector<T::BIT_COUNT> bit_vector_t;
};

typedef ::testing::Types<TestParams<2> > BitVectorTypes;
TYPED_TEST_CASE(BitVectorTest, BitVectorTypes);

TYPED_TEST(BitVectorTest, resize) {
  typename TestFixture::bit_vector_t bit_vector;

  size_t size = 2357;

  bit_vector.resize(size);
  ASSERT_EQ(bit_vector.size(), size);
  ASSERT_EQ(bit_vector.get_data().length(), static_cast<size_t>(std::ceil(
    static_cast<double>(size) / ((sizeof(8.0) / bit_vector.BIT_COUNT)))));
}

TYPED_TEST(BitVectorTest, clear) {
  typename TestFixture::bit_vector_t bit_vector;

  bit_vector.resize(123);
  bit_vector.clear();
  ASSERT_EQ(0ull, bit_vector.size());
  ASSERT_EQ(0ull, bit_vector.get_data().length());
}

TYPED_TEST(BitVectorTest, bit_order) {
  typename TestFixture::bit_vector_t bit_vector;
  bit_vector.resize(1);

  uint8_t value = 1;
  bit_vector[0] = value;

  value <<= (8 - bit_vector.BIT_COUNT);
  ASSERT_EQ(value, bit_vector.get_data()[0]);
}

TYPED_TEST(BitVectorTest, get_set) {
  typename TestFixture::bit_vector_t bit_vector;
  std::vector<uint64_t> ref;

  uint64_t radix = 1 << bit_vector.BIT_COUNT;

  size_t size = 1024;
  bit_vector.resize(size);
  ref.resize(size);
  for (size_t i = 0; i < size; ++i) {
    uint64_t v = rand() % radix;
    ref[i] = v;
    bit_vector[i] = v;
  }

  const typename TestFixture::bit_vector_t &const_bit_vector(bit_vector);
  for (size_t i = 0; i < size; ++i) {
    ASSERT_EQ(ref[i], bit_vector[i]);
    ASSERT_EQ(ref[i], const_bit_vector[i]);
  }
}
