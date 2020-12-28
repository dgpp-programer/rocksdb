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
class SliceTransform;
class BlockBasedTable;
class Block;
class FilterBlockReader;
class ParsedFullFilterBlock;
class Generic;
class FilePrefetchBuffer;
class BlockHandle;
class BlockFetcher;
class DataBlockIter;
class DBImpl;
class Version;
class TableCache;

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
  virtual void IterateNextDone(AsyncContext&) {}
};

typedef void (BlockFetcher::*read_complete_cb)(AsyncContext &ctx);

// TODO make sure all pointer refer to no function variable
struct AsyncContext {
  struct {
    Slice* key;
    PinnableSlice* value;
    ColumnFamilyHandle* cf;
    uint64_t offset;
    uint64_t length;
    uint64_t start_time;
    Slice* result;
    char* scratch;
    read_complete_cb read_complete;
    std::function<void(AsyncContext&)> callback;
  } get;

  struct {
    ColumnFamilyData *cfd;
    SuperVersion* sv;
    DBImpl* db_impl;
    SequenceNumber max_covering_tombstone_seq;
    struct {
      std::unique_ptr<LookupKey> lkey;
      Slice internal_key;
      Slice user_key;
    } key_info;
    std::unique_ptr<PinnedIteratorsManager> pinned_iters_mgr;
    std::unique_ptr<MergeContext> merge_context;
    std::unique_ptr<GetContext> getCtx;
    std::unique_ptr<FilePicker> fp;
  } version;

  struct {
    Cache::Handle* handle;
    TableCache* table_cache;
  } cache;

  struct {
    bool key_may_match;
    bool skip_filters;
    bool for_compaction;
    bool read_contents_no_cache;
    bool second_level; // used by PartitionedFilterBlockReader
    uint64_t block_offset;
    BlockType block_type;
    FilePrefetchBuffer* prefetch_buffer;
    BlockHandle* handle; // TODO change to BlockHandle?
    char* cache_key; // TODO when to delete
    char* compressed_cache_key;
    Slice key;
    Slice ckey;
    UncompressionDict* uncompression_dict;
    std::unique_ptr<BlockCacheLookupContext> lookup_context;
    std::shared_ptr<const SliceTransform> prefix_extractor;
    std::unique_ptr<BlockFetcher> block_fetcher;
    std::unique_ptr<BlockContents> raw_block_contents;
    AsyncCallback* async_cb;
    IteratorCallback* iter_cb;
    union {
      std::unique_ptr<CachableEntry<Generic>> cache_entry;
      std::unique_ptr<CachableEntry<BlockContents>> contents; // BlockBasedFilterBlockReader
      std::unique_ptr<CachableEntry<Block>> block; // BinarySearchIndexReader & PartitionedFilterBlockReader
      std::unique_ptr<CachableEntry<ParsedFullFilterBlock>> full_filter_block; // FullFilterBlockReader
    } retrieve_block; // TODO do release
    std::unique_ptr<InternalIteratorBase<IndexValue>> index_iter;
    std::unique_ptr<DataBlockIter> data_iter;
  } reader;

  ReadOptions* options;
  Status status;
};

}  //  namespace rocksdb
