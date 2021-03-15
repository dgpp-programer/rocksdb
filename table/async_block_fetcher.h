//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "memory/memory_allocator.h"
#include "rocksdb/block_type.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/format.h"

namespace rocksdb {

// PersistentCache not support yet
class AsyncBlockFetcher {
 public:
  AsyncBlockFetcher(const BlockBasedTable* table, AsyncContext* context)
      : table_(const_cast<BlockBasedTable*>(table)),
        context_(context) { }

  void ReadBlockContentsAsync();

  void ReadFromCacheCallback();

  void ReadBlockContentsCallback();

  void PrefetchDone();

  void reset(const BlockBasedTable* table, AsyncContext* context) {
    table_ = const_cast<BlockBasedTable*>(table);
    context_ = context;
    slice_.clear();
    used_buf_ = nullptr;
    heap_buf_.reset();
    got_from_prefetch_buffer_ = false;
  }

  // Compress not support yet
  CompressionType get_compression_type() const { return CompressionType::kNoCompression; }

 private:
  static const uint32_t kDefaultStackBufferSize = 5000;

  Slice slice_;
  char* used_buf_ = nullptr;
  CacheAllocationPtr heap_buf_;
  bool got_from_prefetch_buffer_ = false;
  BlockBasedTable* table_;
  AsyncContext* context_;

  void GetFromPrefetchBufferCallback();

  void PrepareBufferForBlockFromFile();
  // Copy content from used_buf_ to new heap buffer.
  void CopyBufferToHeap();
  void GetBlockContents();
  void CheckBlockChecksum();
  void ReadBlockContentsDone();
};
}  // namespace rocksdb
