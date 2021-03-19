//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/memory_allocator.h"

extern "C" {
#include "spdk/env.h"
}

namespace rocksdb {

// Memory Allocator based on DPDK heap
// Mainly used by read/write zero copy
class SpdkMemoryAllocator : public MemoryAllocator {
 public:
  SpdkMemoryAllocator(SpdkAllocatorOptions& options) : options_(options) {
    if (options_.use_mempool) {
      mem_pool = spdk_mempool_create("spdk_memory_allocator",
          options.count, options.ele_size, options.cache_size,
          options.socket_id);
    }
  }

  ~SpdkMemoryAllocator() {
    if (mem_pool) {
      spdk_mempool_free(mem_pool);
      mem_pool = nullptr;
    }
  }

  const char* Name() const override { return "SpdkMemoryAllocator"; }

  void* Allocate(size_t size) override {
    if (options_.use_mempool && size <= options_.ele_size) {
      size_t count = 0;
      void* buf;
      do {
        buf = spdk_mempool_get(mem_pool);
        if (buf) {
          break;
        }
        if (count++ == options_.retry_number) {
          // TODO chenxu14 add some log
          return NULL;
        }
        usleep(1000);
      } while (true);
      return buf;
    } else {
      return spdk_malloc(size, 0, NULL, options_.socket_id, SPDK_MALLOC_DMA);
    }
  }

  void Deallocate(void* p, size_t size) override {
    if (options_.use_mempool && size <= options_.ele_size) {
      spdk_mempool_put(mem_pool, p);
    } else {
      spdk_free(p);
    }
  }

  size_t ElementSize() const override {
    return options_.ele_size;
  }

 private:
  const SpdkAllocatorOptions options_;
  struct spdk_mempool *mem_pool;
};

Status NewSpdkMemoryAllocator(SpdkAllocatorOptions& options,
    std::shared_ptr<MemoryAllocator>* memory_allocator) {
  memory_allocator->reset(new SpdkMemoryAllocator(options));
  return Status::OK();
}

}  // namespace rocksdb
