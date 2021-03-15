//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/async_block_fetcher.h"

#include <cinttypes>
#include <string>
#include <type_traits>

#include "logging/logging.h"
#include "memory/memory_allocator.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/env.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/format.h"
#include "table/persistent_cache_helper.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/xxhash.h"

namespace rocksdb {

inline void AsyncBlockFetcher::CheckBlockChecksum() {
  // Check the crc of the type and the block contents
  if (context_->options->verify_checksums) {
    const char* data = slice_.data();  // Pointer to where Read put the data
    PERF_TIMER_GUARD(block_checksum_time);
    uint32_t value = DecodeFixed32(data + context_->read.handle.size() + 1);
    uint32_t actual = 0;
    auto rep_t = table_->get_rep();
    switch (rep_t->footer.checksum()) {
      case kNoChecksum:
        break;
      case kCRC32c:
        value = crc32c::Unmask(value);
        actual = crc32c::Value(data, context_->read.handle.size() + 1);
        break;
      case kxxHash:
        actual = XXH32(data, static_cast<int>(context_->read.handle.size()) + 1, 0);
        break;
      case kxxHash64:
        actual = static_cast<uint32_t>(
            XXH64(data, static_cast<int>(context_->read.handle.size()) + 1, 0) &
            uint64_t{0xffffffff});
        break;
      default:
        context_->status = Status::Corruption(
            "unknown checksum type " + ToString(rep_t->footer.checksum()) + " in " +
            rep_t->file->file_name() + " offset " + ToString(context_->read.handle.offset()) +
            " size " + ToString(context_->read.handle.size()));
    }
    if (context_->status.ok() && actual != value) {
      context_->status = Status::Corruption(
          "block checksum mismatch: expected " + ToString(actual) + ", got " +
          ToString(value) + "  in " + rep_t->file->file_name() + " offset " +
          ToString(context_->read.handle.offset()) + " size " + ToString(context_->read.handle.size()));
    }
  }
}

void AsyncBlockFetcher::ReadFromCacheCallback() {
  if (context_->read.prefetch_buf_hit) {
    got_from_prefetch_buffer_ = true;
    CheckBlockChecksum();
    if (context_->status.ok()) {
      used_buf_ = const_cast<char*>(slice_.data());
    }
  }
  GetFromPrefetchBufferCallback();
}

inline void AsyncBlockFetcher::PrepareBufferForBlockFromFile() {
  // refer __get_page_parameters to do block alignment
  uint32_t lba_size = kDefaultPageSize; // TODO chenxu14 consider spdk_bs_get_io_unit_size
  uint64_t start_lba = context_->read.handle.offset() / lba_size;
  uint64_t end_lba = (context_->read.handle.offset() +
      context_->read.handle.size() + kBlockTrailerSize - 1) / lba_size;
  uint64_t num_lba = (end_lba - start_lba + 1);
  heap_buf_ = AllocateBlock(num_lba * lba_size, table_->get_rep()->table_options.memory_allocator.get());
  used_buf_ = heap_buf_.get();
}

inline void AsyncBlockFetcher::CopyBufferToHeap() {
  assert(used_buf_ != heap_buf_.get());
  heap_buf_ = AllocateBlock(context_->read.handle.size() + kBlockTrailerSize,
      table_->get_rep()->table_options.memory_allocator.get());
  memcpy(heap_buf_.get(), used_buf_, context_->read.handle.size() + kBlockTrailerSize);
}

inline void AsyncBlockFetcher::GetBlockContents() {
  if (slice_.data() != used_buf_) {
    // the slice content is not the buffer provided
    context_->read.raw_block_contents->reset(Slice(slice_.data(), context_->read.handle.size()));
  } else {
    // page can be either uncompressed or compressed, the buffer either stack
    // or heap provided. Refer to https://github.com/facebook/rocksdb/pull/4096
    if (got_from_prefetch_buffer_) {
      CopyBufferToHeap();
    }
    context_->read.raw_block_contents->reset(std::move(heap_buf_), context_->read.handle.size());
  }
#ifndef NDEBUG
  context_->read.raw_block_contents->is_raw_block = true;
#endif
}

void AsyncBlockFetcher::ReadBlockContentsDone() {
  if (context_->read.block_type == BlockType::kFilter) {
    if (context_->read.second_level
        || table_->get_rep()->filter_type == BlockBasedTable::Rep::FilterType::kFullFilter) {
      CachableEntry<ParsedFullFilterBlock> *block;
      return context_->read.read_contents_no_cache
          ? table_->ReadBlockContentsDone(*context_, block)
          : table_->ReadBlockContentsCallback(*context_, block);
    }
  }
  CachableEntry<Block> *block;
  return context_->read.read_contents_no_cache
      ? table_->ReadBlockContentsDone(*context_, block)
      : table_->ReadBlockContentsCallback(*context_, block);
}

void AsyncBlockFetcher::PrefetchDone() {
  if (context_->status.ok()) {
    context_->read.prefetch_buffer->buffer_offset_ = context_->read.offset - context_->read.chunk_len;
    context_->read.prefetch_buffer->buffer_.Size(static_cast<size_t>(context_->read.chunk_len)
        + context_->read.result->size());
  }
  context_->read.prefetch_buffer->PrefetchCallback(*context_);
}

void AsyncBlockFetcher::ReadBlockContentsCallback() {
  PERF_COUNTER_ADD(block_read_count, 1);
  switch (context_->read.block_type) {
    case BlockType::kFilter:
      PERF_COUNTER_ADD(filter_block_read_count, 1);
      break;
    case BlockType::kCompressionDictionary:
      PERF_COUNTER_ADD(compression_dict_block_read_count, 1);
      break;
    case BlockType::kIndex:
      PERF_COUNTER_ADD(index_block_read_count, 1);
      break;
    default:
      break;
  }
  PERF_COUNTER_ADD(block_read_byte, context_->read.handle.size() + kBlockTrailerSize);
  if (!context_->status.ok()) {
    return ReadBlockContentsDone();
  }
  if (slice_.size() != context_->read.handle.size() + kBlockTrailerSize) {
    context_->status = Status::Corruption("truncated block read from " +
        table_->get_rep()->file->file_name() + " offset " + ToString(context_->read.handle.offset()) + ", expected " +
        ToString(context_->read.handle.size() + kBlockTrailerSize) + " bytes, got " + ToString(slice_.size()));
    return ReadBlockContentsDone();
  }
  CheckBlockChecksum();
  if (context_->status.ok()) {
    GetBlockContents();
  }
  return ReadBlockContentsDone();
}

void AsyncBlockFetcher::GetFromPrefetchBufferCallback() {
  if (got_from_prefetch_buffer_) {
    if (!context_->status.ok()) {
      return ReadBlockContentsDone();
    }
  } else {
    PrepareBufferForBlockFromFile();
    context_->read.offset = context_->read.handle.offset();
    context_->read.length = context_->read.handle.size() + kBlockTrailerSize;
    context_->read.result = &slice_;
    context_->read.scratch = used_buf_;
    context_->read.read_complete = &AsyncBlockFetcher::ReadBlockContentsCallback;
    return table_->get_rep()->file->ReadAsync(*context_);
  }

  GetBlockContents();
  return ReadBlockContentsDone();
}

void AsyncBlockFetcher::ReadBlockContentsAsync() {
  if (context_->read.prefetch_buffer != nullptr) {
    context_->read.offset = context_->read.handle.offset();
    context_->read.length = context_->read.handle.size() + kBlockTrailerSize;
    context_->read.result = &slice_;
    return context_->read.prefetch_buffer->TryReadFromCacheAsync(*context_);
  }
  GetFromPrefetchBufferCallback();
}

}  // namespace rocksdb
