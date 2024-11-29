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
  if (context_->op.scan.args.prefetch_buf_hit) {
    got_from_prefetch_buffer_ = true;
    CheckBlockChecksum();
  }
  GetFromPrefetchBufferCallback();
}

inline void AsyncBlockFetcher::PrepareBufferForBlockFromFile() {
  // refer __get_page_parameters to do block alignment
  uint32_t lba_size = table_->get_rep()->file->file()->GetRequiredBufferAlignment();
  uint64_t start_lba = context_->read.handle.offset() / lba_size;
  uint64_t end_lba = (context_->read.handle.offset() +
      context_->read.handle.size() + kBlockTrailerSize - 1) / lba_size;
  uint64_t num_lba = (end_lba - start_lba + 1);
  heap_buf_ = AllocateBlock(num_lba * lba_size, table_->get_rep()->table_options.memory_allocator.get());
}

inline void AsyncBlockFetcher::GetBlockContents() {
  // [NOTICE] chenxu14 skip CopyBufferToHeap, is it really need?
  context_->read.raw_block_contents->reset(std::move(heap_buf_),
      Slice(slice_.data(), context_->read.handle.size()));
#ifndef NDEBUG
  context_->read.raw_block_contents->is_raw_block = true;
#endif
}

void AsyncBlockFetcher::ReadBlockContentsDone() {
  //对于第一次读，需要去文件读布隆过滤器，所以这里block_type为kFilter，且fileter为kFullFilter，read_contents_no_cache为false
  if (context_->read.block_type == BlockType::kFilter) {
    if (context_->read.second_level
        || table_->get_rep()->filter_type == BlockBasedTable::Rep::FilterType::kFullFilter) {
      CachableEntry<ParsedFullFilterBlock> *block = nullptr;
      //这里就是根据 read_contents_no_cache 判断缓存读的数据是否在缓存中存在，存在就直接走ReadBlockContentsDone
      //不存在就走 ReadBlockContentsCallback，先将数据丢到缓存，然后走缓存读到的逻辑回调
      //最终都是走 FullFilterBlockReader::RetrieveBlockDone进入到下一步读index环节
      return context_->read.read_contents_no_cache
          ? table_->ReadBlockContentsDone(*context_, block)
          : table_->ReadBlockContentsCallback(*context_, block);
    }
  }
  CachableEntry<Block> *block = nullptr;
  return context_->read.read_contents_no_cache
      ? table_->ReadBlockContentsDone(*context_, block)
      : table_->ReadBlockContentsCallback(*context_, block);
}

void AsyncBlockFetcher::PrefetchDone() {
  if (context_->status.ok()) {
    context_->read.prefetch_buffer->buffer_offset_ =
        context_->read.offset - context_->op.scan.args.chunk_len;
    context_->read.prefetch_buffer->buffer_.Size(
        static_cast<size_t>(context_->op.scan.args.chunk_len) + context_->read.result->size());
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
  // got_from_prefetch_buffer_初始化为false
  if (got_from_prefetch_buffer_) {
    if (!context_->status.ok()) {
      return ReadBlockContentsDone();
    }
    GetBlockContents();
    return ReadBlockContentsDone();
  } else {
    PrepareBufferForBlockFromFile(); // allocate heap_buf_ as payload
    context_->read.offset = context_->read.handle.offset();
    context_->read.length = context_->read.handle.size() + kBlockTrailerSize;
    context_->read.result = &slice_;
    context_->read.scratch = heap_buf_.get();
    //设置异步读回调函数
    context_->read.read_complete = &AsyncBlockFetcher::ReadBlockContentsCallback;
    //这里取异步读数据
    return table_->get_rep()->file->ReadAsync(*context_);
  }
}

void AsyncBlockFetcher::ReadBlockContentsAsync() {
  //第一次的时候 prefetch_buffer为nullptr
  if (context_->read.prefetch_buffer != nullptr) {
    context_->read.offset = context_->read.handle.offset();
    context_->read.length = context_->read.handle.size() + kBlockTrailerSize;
    context_->read.result = &slice_;
    return context_->read.prefetch_buffer->TryReadFromCacheAsync(*context_);
  }
  GetFromPrefetchBufferCallback();
}

}  // namespace rocksdb
