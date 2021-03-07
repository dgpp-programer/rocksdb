//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include "rocksdb/memory_allocator.h"

namespace rocksdb {

struct CustomDeleter {
  CustomDeleter(MemoryAllocator* a = nullptr, size_t s = 0)
    : allocator(a), size(s) {}

  void operator()(char* ptr) const {
    if (allocator) {
      allocator->Deallocate(reinterpret_cast<void*>(ptr), size);
    } else {
      delete[] ptr;
    }
  }

  MemoryAllocator* allocator;
  size_t size;
};

using CacheAllocationPtr = std::unique_ptr<char[], CustomDeleter>;

inline CacheAllocationPtr AllocateBlock(size_t size,
                                        MemoryAllocator* allocator) {
  CustomDeleter deleter(allocator, size);
  if (allocator) {
    auto block = reinterpret_cast<char*>(allocator->Allocate(size));
    return CacheAllocationPtr(block, deleter);
  }
  return CacheAllocationPtr(new char[size], deleter);
}

}  // namespace rocksdb
