// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>
#include <vector>
#include "rocksdb/slice.h"
#include "rocksdb/types.h"
#include "rocksdb/cache.h"
#include "rocksdb/block_type.h"

namespace rocksdb {

class ColumnFamilyHandle;
class ColumnFamilyData;
class GetContext;
class FilePicker;
class MergeContext;
class LookupKey;
class PinnedIteratorsManager;
class Block;
class ParsedFullFilterBlock;
class Generic;
class FilePrefetchBuffer;
class BlockHandle;
class BlockFetcher;
class DataBlockIter;
class DBImpl;
class ReadCallback;
class Iterator;

template <class T> class CachableEntry;
template <class TValue> class InternalIteratorBase;

struct ReadOptions;
struct SuperVersion;
struct BlockCacheLookupContext;
struct BlockContents;
struct UncompressionDict;
struct IndexValue;
struct AsyncContext;

class AsyncCallback {
public:
  virtual ~AsyncCallback() = default;
  virtual void RetrieveBlockDone(AsyncContext& context) = 0;
};

class IteratorCallback {
public:
  virtual ~IteratorCallback() = default;
  virtual void SeekDone(AsyncContext&) = 0;
  virtual void NextDone(AsyncContext& context) {
    SeekDone(context);
  }
};

typedef void (BlockFetcher::*read_complete_cb)(AsyncContext &ctx);

// TODO make sure all pointer refer to no function variable

struct ReadContext {
  DBImpl* db_impl;
  ColumnFamilyData *cfd;
  SuperVersion* sv;
  std::unique_ptr<GetContext> getCtx;
  struct {
    std::unique_ptr<LookupKey> lkey;
    Slice internal_key;
    Slice user_key;
  } key_info;
  bool key_may_match;
  bool skip_filters;
  bool skip_seek; // when build iterator, no need to do index seek
  bool for_compaction;
  bool read_contents_no_cache;
  bool second_level; // used by PartitionedFilterBlockReader
  uint64_t block_offset;
  BlockType block_type;
  FilePrefetchBuffer* prefetch_buffer;
  bool prefetch_buf_hit;
  uint64_t chunk_len;
  BlockHandle* handle;
  char* cache_key; // TODO when to delete
  char* compressed_cache_key;
  Slice key;
  Slice ckey;
  UncompressionDict* uncompression_dict;
  // TODO chenxu14 consider add reset method with pointer Entry
  // or use SPDK's memory pool
  std::unique_ptr<BlockCacheLookupContext> lookup_context;
  std::unique_ptr<BlockFetcher> block_fetcher;
  std::unique_ptr<BlockContents> raw_block_contents;
  AsyncCallback* async_cb;
  IteratorCallback* index_iter_cb;
  bool index_iter_seek;
  union {
    std::unique_ptr<CachableEntry<Generic>> cache_entry;
    std::unique_ptr<CachableEntry<BlockContents>> contents;
    std::unique_ptr<CachableEntry<Block>> block;
    std::unique_ptr<CachableEntry<ParsedFullFilterBlock>> full_filter_block;
  } retrieve_block;
  std::unique_ptr<InternalIteratorBase<IndexValue>> index_iter;
  std::unique_ptr<DataBlockIter> data_iter;
  // used by RetrieveBlockAsync
  uint64_t offset;
  uint64_t length;
  Slice* result;
  char* scratch;
  read_complete_cb read_complete;
};

struct GetContextArgs {
  SequenceNumber max_covering_tombstone_seq;
  std::unique_ptr<PinnedIteratorsManager> pinned_iters_mgr;
  std::unique_ptr<MergeContext> merge_context;
  std::unique_ptr<FilePicker> fp;
  Cache::Handle* cache_handle;
};

struct ScanContextArgs {
  uint64_t child_index;
  IteratorCallback* merging_iter_cb;
  ReadCallback* read_cb;
  IteratorCallback* iter_cb;
  bool iter_seek;
};

/**
 * NOT THREAD SAFE
 * Can only be used by one thread at a time
 * TODO chenxu14 cache line align
 */
struct AsyncContext {
  uint64_t start_time;
  ColumnFamilyHandle* cf;
  ReadOptions* options;
  Status status;
  struct ReadContext read;
  struct {
    struct {
      Slice* key;
      PinnableSlice* value;
      std::function<void(AsyncContext&)> callback;
      struct GetContextArgs args;
    } get;
    struct {
      Slice* startKey; // TODO chenxu14 change to Slice
      std::unique_ptr<Iterator> iterator;
      std::function<void(AsyncContext&)> seek_callback;
      std::function<void(AsyncContext&)> next_callback;
      int32_t next_counter;
      struct ScanContextArgs args;
    } scan;
  } op;
};

}  //  namespace rocksdb
