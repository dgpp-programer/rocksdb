//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/full_filter_block.h"
#include <array>

#include "monitoring/perf_context_imp.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/filter_policy.h"
#include "table/block_based/block_based_table_reader.h"
#include "util/coding.h"

namespace rocksdb {

FullFilterBlockBuilder::FullFilterBlockBuilder(
    const SliceTransform* _prefix_extractor, bool whole_key_filtering,
    FilterBitsBuilder* filter_bits_builder)
    : prefix_extractor_(_prefix_extractor),
      whole_key_filtering_(whole_key_filtering),
      last_whole_key_recorded_(false),
      last_prefix_recorded_(false),
      num_added_(0) {
  assert(filter_bits_builder != nullptr);
  filter_bits_builder_.reset(filter_bits_builder);
}

void FullFilterBlockBuilder::Add(const Slice& key) {
  const bool add_prefix = prefix_extractor_ && prefix_extractor_->InDomain(key);
  if (whole_key_filtering_) {
    if (!add_prefix) {
      AddKey(key);
    } else {
      // if both whole_key and prefix are added to bloom then we will have whole
      // key and prefix addition being interleaved and thus cannot rely on the
      // bits builder to properly detect the duplicates by comparing with the
      // last item.
      Slice last_whole_key = Slice(last_whole_key_str_);
      if (!last_whole_key_recorded_ || last_whole_key.compare(key) != 0) {
        AddKey(key);
        last_whole_key_recorded_ = true;
        last_whole_key_str_.assign(key.data(), key.size());
      }
    }
  }
  if (add_prefix) {
    AddPrefix(key);
  }
}

// Add key to filter if needed
inline void FullFilterBlockBuilder::AddKey(const Slice& key) {
  filter_bits_builder_->AddKey(key);
  num_added_++;
}

// Add prefix to filter if needed
void FullFilterBlockBuilder::AddPrefix(const Slice& key) {
  Slice prefix = prefix_extractor_->Transform(key);
  if (whole_key_filtering_) {
    // if both whole_key and prefix are added to bloom then we will have whole
    // key and prefix addition being interleaved and thus cannot rely on the
    // bits builder to properly detect the duplicates by comparing with the last
    // item.
    Slice last_prefix = Slice(last_prefix_str_);
    if (!last_prefix_recorded_ || last_prefix.compare(prefix) != 0) {
      AddKey(prefix);
      last_prefix_recorded_ = true;
      last_prefix_str_.assign(prefix.data(), prefix.size());
    }
  } else {
    AddKey(prefix);
  }
}

void FullFilterBlockBuilder::Reset() {
  last_whole_key_recorded_ = false;
  last_prefix_recorded_ = false;
}

Slice FullFilterBlockBuilder::Finish(const BlockHandle& /*tmp*/,
                                     Status* status) {
  Reset();
  // In this impl we ignore BlockHandle
  *status = Status::OK();
  if (num_added_ != 0) {
    num_added_ = 0;
    return filter_bits_builder_->Finish(&filter_data_);
  }
  return Slice();
}

FullFilterBlockReader::FullFilterBlockReader(
    const BlockBasedTable* t,
    CachableEntry<ParsedFullFilterBlock>&& filter_block)
    : FilterBlockReaderCommon(t, std::move(filter_block)) {
  const SliceTransform* const prefix_extractor = table_prefix_extractor();
  if (prefix_extractor) {
    full_length_enabled_ =
        prefix_extractor->FullLengthEnabled(&prefix_extractor_full_length_);
  }
}

void FullFilterBlockReader::RetrieveBlockDone(AsyncContext &context) {
  auto t = const_cast<BlockBasedTable*>(table());
  if (!context.status.ok()) {
    context.read.key_may_match = true;
    return t->GetAsyncCallback(context);
  }
  assert(context.read.retrieve_block.full_filter_block->GetValue());
  FilterBitsReader* const filter_bits_reader =
      context.read.retrieve_block.full_filter_block->GetValue()->filter_bits_reader();

  if (filter_bits_reader) {
    Slice user_key = ExtractUserKey(context.read.key_info.internal_key);
    size_t ts_sz = table()->get_rep()->internal_comparator.user_comparator()->timestamp_size();
    Slice entry = StripTimestampFromUserKey(user_key, ts_sz);

    if (filter_bits_reader->MayMatch(entry)) {
      PERF_COUNTER_ADD(bloom_sst_hit_count, 1);
      context.read.key_may_match = true;
    } else {
      PERF_COUNTER_ADD(bloom_sst_miss_count, 1);
      context.read.key_may_match = false;
    }
  } else {
    // remain the same with block_based filter
    context.read.key_may_match = true;
  }
  context.read.retrieve_block.full_filter_block->Reset();
  t->GetAsyncCallback(context);
}

void FullFilterBlockReader::KeyMayMatchAsync(AsyncContext &context) {
  assert(context.read.block_offset == kNotValid);
  if (!whole_key_filtering()) {
    context.read.key_may_match = true;
    auto t = const_cast<BlockBasedTable*>(table());
    return t->GetAsyncCallback(context);
  }

  if (context.read.retrieve_block.full_filter_block) {
    context.read.retrieve_block.full_filter_block->Reset();
  } else {
    if (context.read.ctx_buffer_size - context.read.ctx_offset > sizeof(CachableEntry<Generic>)) {
      auto mem = (uint8_t *)&context + sizeof(AsyncContext) + context.read.ctx_offset;
      context.read.retrieve_block.full_filter_block.reset(new (mem) CachableEntry<ParsedFullFilterBlock>());
      context.read.ctx_offset += sizeof(CachableEntry<Generic>);
    } else {
      context.read.retrieve_block.full_filter_block.reset(new CachableEntry<ParsedFullFilterBlock>());
    }
  }
  if (!filter_block_.IsEmpty()) { // TODO chenxu14 test cache not find
    context.read.retrieve_block.full_filter_block->SetUnownedValue(filter_block_.GetValue());
    context.status = Status::OK();
    return RetrieveBlockDone(context);
  }

  PERF_TIMER_GUARD(read_filter_block_nanos);
  const BlockBasedTable::Rep* const rep = table_->get_rep();
  assert(rep);

  context.read.async_cb = this;
  context.read.block_type = BlockType::kFilter;
  context.read.prefetch_buffer = nullptr;
  context.read.handle = rep->filter_handle;
  context.read.uncompression_dict = const_cast<UncompressionDict*>(
      &UncompressionDict::GetEmptyDict());
  context.read.for_compaction = false;
  table_->RetrieveBlockAsync(context, context.read.retrieve_block.full_filter_block.get(),
      cache_filter_blocks());
}

bool FullFilterBlockReader::KeyMayMatch(
    const Slice& key, const SliceTransform* /*prefix_extractor*/,
    uint64_t block_offset, const bool no_io,
    const Slice* const /*const_ikey_ptr*/, GetContext* get_context,
    BlockCacheLookupContext* lookup_context) {
#ifdef NDEBUG
  (void)block_offset;
#endif
  assert(block_offset == kNotValid);
  if (!whole_key_filtering()) {
    return true;
  }
  return MayMatch(key, no_io, get_context, lookup_context);
}

std::unique_ptr<FilterBlockReader> FullFilterBlockReader::Create(
    const BlockBasedTable* table, FilePrefetchBuffer* prefetch_buffer,
    bool use_cache, bool prefetch, bool pin,
    BlockCacheLookupContext* lookup_context) {
  assert(table);
  assert(table->get_rep());
  assert(!pin || prefetch);

  CachableEntry<ParsedFullFilterBlock> filter_block;
  if (prefetch || !use_cache) {
    const Status s = ReadFilterBlock(table, prefetch_buffer, ReadOptions(),
                                     use_cache, nullptr /* get_context */,
                                     lookup_context, &filter_block);
    if (!s.ok()) {
      return std::unique_ptr<FilterBlockReader>();
    }

    if (use_cache && !pin) {
      filter_block.Reset();
    }
  }

  return std::unique_ptr<FilterBlockReader>(
      new FullFilterBlockReader(table, std::move(filter_block)));
}

bool FullFilterBlockReader::PrefixMayMatch(
    const Slice& prefix, const SliceTransform* /* prefix_extractor */,
    uint64_t block_offset, const bool no_io,
    const Slice* const /*const_ikey_ptr*/, GetContext* get_context,
    BlockCacheLookupContext* lookup_context) {
#ifdef NDEBUG
  (void)block_offset;
#endif
  assert(block_offset == kNotValid);
  return MayMatch(prefix, no_io, get_context, lookup_context);
}

bool FullFilterBlockReader::MayMatch(
    const Slice& entry, bool no_io, GetContext* get_context,
    BlockCacheLookupContext* lookup_context) const {
  CachableEntry<ParsedFullFilterBlock> filter_block;

  const Status s =
      GetOrReadFilterBlock(no_io, get_context, lookup_context, &filter_block);
  if (!s.ok()) {
    return true;
  }

  assert(filter_block.GetValue());

  FilterBitsReader* const filter_bits_reader =
      filter_block.GetValue()->filter_bits_reader();

  if (filter_bits_reader) {
    if (filter_bits_reader->MayMatch(entry)) {
      PERF_COUNTER_ADD(bloom_sst_hit_count, 1);
      return true;
    } else {
      PERF_COUNTER_ADD(bloom_sst_miss_count, 1);
      return false;
    }
  }
  return true;  // remain the same with block_based filter
}

void FullFilterBlockReader::KeysMayMatch(
    MultiGetRange* range, const SliceTransform* /*prefix_extractor*/,
    uint64_t block_offset, const bool no_io,
    BlockCacheLookupContext* lookup_context) {
#ifdef NDEBUG
  (void)range;
  (void)block_offset;
#endif
  assert(block_offset == kNotValid);
  if (!whole_key_filtering()) {
    // Simply return. Don't skip any key - consider all keys as likely to be
    // present
    return;
  }
  MayMatch(range, no_io, nullptr, lookup_context);
}

void FullFilterBlockReader::PrefixesMayMatch(
    MultiGetRange* range, const SliceTransform* prefix_extractor,
    uint64_t block_offset, const bool no_io,
    BlockCacheLookupContext* lookup_context) {
#ifdef NDEBUG
  (void)range;
  (void)block_offset;
#endif
  assert(block_offset == kNotValid);
  MayMatch(range, no_io, prefix_extractor, lookup_context);
}

void FullFilterBlockReader::MayMatch(
    MultiGetRange* range, bool no_io, const SliceTransform* prefix_extractor,
    BlockCacheLookupContext* lookup_context) const {
  CachableEntry<ParsedFullFilterBlock> filter_block;

  const Status s = GetOrReadFilterBlock(no_io, range->begin()->get_context,
                                        lookup_context, &filter_block);
  if (!s.ok()) {
    return;
  }

  assert(filter_block.GetValue());

  FilterBitsReader* const filter_bits_reader =
      filter_block.GetValue()->filter_bits_reader();

  if (!filter_bits_reader) {
    return;
  }

  // We need to use an array instead of autovector for may_match since
  // &may_match[0] doesn't work for autovector<bool> (compiler error). So
  // declare both keys and may_match as arrays, which is also slightly less
  // expensive compared to autovector
  std::array<Slice*, MultiGetContext::MAX_BATCH_SIZE> keys;
  std::array<bool, MultiGetContext::MAX_BATCH_SIZE> may_match = {{true}};
  autovector<Slice, MultiGetContext::MAX_BATCH_SIZE> prefixes;
  int num_keys = 0;
  MultiGetRange filter_range(*range, range->begin(), range->end());
  for (auto iter = filter_range.begin(); iter != filter_range.end(); ++iter) {
    if (!prefix_extractor) {
      keys[num_keys++] = &iter->ukey;
    } else if (prefix_extractor->InDomain(iter->ukey)) {
      prefixes.emplace_back(prefix_extractor->Transform(iter->ukey));
      keys[num_keys++] = &prefixes.back();
    } else {
      filter_range.SkipKey(iter);
    }
  }

  filter_bits_reader->MayMatch(num_keys, &keys[0], &may_match[0]);

  int i = 0;
  for (auto iter = filter_range.begin(); iter != filter_range.end(); ++iter) {
    if (!may_match[i]) {
      // Update original MultiGet range to skip this key. The filter_range
      // was temporarily used just to skip keys not in prefix_extractor domain
      range->SkipKey(iter);
      PERF_COUNTER_ADD(bloom_sst_miss_count, 1);
    } else {
      // PERF_COUNTER_ADD(bloom_sst_hit_count, 1);
      PerfContext* perf_ctx = get_perf_context();
      perf_ctx->bloom_sst_hit_count++;
    }
    ++i;
  }
}

size_t FullFilterBlockReader::ApproximateMemoryUsage() const {
  size_t usage = ApproximateFilterBlockMemoryUsage();
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  usage += malloc_usable_size(const_cast<FullFilterBlockReader*>(this));
#else
  usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
  return usage;
}

bool FullFilterBlockReader::RangeMayExist(
    const Slice* iterate_upper_bound, const Slice& user_key,
    const SliceTransform* prefix_extractor, const Comparator* comparator,
    const Slice* const const_ikey_ptr, bool* filter_checked,
    bool need_upper_bound_check, BlockCacheLookupContext* lookup_context) {
  if (!prefix_extractor || !prefix_extractor->InDomain(user_key)) {
    *filter_checked = false;
    return true;
  }
  Slice prefix = prefix_extractor->Transform(user_key);
  if (need_upper_bound_check &&
      !IsFilterCompatible(iterate_upper_bound, prefix, comparator)) {
    *filter_checked = false;
    return true;
  } else {
    *filter_checked = true;
    return PrefixMayMatch(prefix, prefix_extractor, kNotValid, false,
                          const_ikey_ptr, /* get_context */ nullptr,
                          lookup_context);
  }
}

bool FullFilterBlockReader::IsFilterCompatible(
    const Slice* iterate_upper_bound, const Slice& prefix,
    const Comparator* comparator) const {
  // Try to reuse the bloom filter in the SST table if prefix_extractor in
  // mutable_cf_options has changed. If range [user_key, upper_bound) all
  // share the same prefix then we may still be able to use the bloom filter.
  const SliceTransform* const prefix_extractor = table_prefix_extractor();
  if (iterate_upper_bound != nullptr && prefix_extractor) {
    if (!prefix_extractor->InDomain(*iterate_upper_bound)) {
      return false;
    }
    Slice upper_bound_xform = prefix_extractor->Transform(*iterate_upper_bound);
    // first check if user_key and upper_bound all share the same prefix
    if (!comparator->Equal(prefix, upper_bound_xform)) {
      // second check if user_key's prefix is the immediate predecessor of
      // upper_bound and have the same length. If so, we know for sure all
      // keys in the range [user_key, upper_bound) share the same prefix.
      // Also need to make sure upper_bound are full length to ensure
      // correctness
      if (!full_length_enabled_ ||
          iterate_upper_bound->size() != prefix_extractor_full_length_ ||
          !comparator->IsSameLengthImmediateSuccessor(prefix,
                                                      *iterate_upper_bound)) {
        return false;
      }
    }
    return true;
  } else {
    return false;
  }
}

}  // namespace rocksdb
