//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/dbformat.h"
#include "rocksdb/types.h"
#include "table/iterator_wrapper.h"
#include "table/iter_heap.h"
#include "util/heap.h"
#include "db/pinned_iterators_manager.h"

namespace rocksdb {

// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
namespace {
typedef BinaryHeap<IteratorWrapper*, MaxIteratorComparator> MergerMaxIterHeap;
typedef BinaryHeap<IteratorWrapper*, MinIteratorComparator> MergerMinIterHeap;
}  // namespace

const size_t kNumIterReserve = 4;

class Comparator;
class Env;
class Arena;
template <class TValue>
class InternalIteratorBase;
using InternalIterator = InternalIteratorBase<Slice>;

// Return an iterator that provided the union of the data in
// children[0,n-1].  Takes ownership of the child iterators and
// will delete them when the result iterator is deleted.
//
// The result does no duplicate suppression.  I.e., if a particular
// key is present in K child iterators, it will be yielded K times.
//
// REQUIRES: n >= 0
extern InternalIterator* NewMergingIterator(
    const InternalKeyComparator* comparator, InternalIterator** children, int n,
    Arena* arena = nullptr, bool prefix_seek_mode = false);

class MergingIterator : public InternalIterator, public IteratorCallback {
 public:
  MergingIterator(const InternalKeyComparator* comparator,
                  InternalIterator** children, int n, bool is_arena_mode,
                  bool prefix_seek_mode)
      : is_arena_mode_(is_arena_mode),
        comparator_(comparator),
        current_(nullptr),
        direction_(kForward),
        minHeap_(comparator_),
        prefix_seek_mode_(prefix_seek_mode),
        pinned_iters_mgr_(nullptr) {
    children_.resize(n);
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
    for (auto& child : children_) {
      AddToMinHeapOrCheckStatus(&child);
    }
    current_ = CurrentForward();
  }

  void considerStatus(Status s) {
    if (!s.ok() && status_.ok()) {
      status_ = s;
    }
  }

  virtual void AddIterator(InternalIterator* iter) {
    assert(direction_ == kForward);
    children_.emplace_back(iter);
    if (pinned_iters_mgr_) {
      iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
    auto new_wrapper = children_.back();
    AddToMinHeapOrCheckStatus(&new_wrapper);
    if (new_wrapper.Valid()) {
      current_ = CurrentForward();
    }
  }

  ~MergingIterator() override {
    for (auto& child : children_) {
      child.DeleteIter(is_arena_mode_);
    }
  }

  bool Valid() const override { return current_ != nullptr && status_.ok(); }

  Status status() const override { return status_; }

  void SeekToFirstAsync(AsyncContext& context) override {
    if (children_.size() > 0) {
      ClearHeaps();
      context.status = Status::OK();
      context.op.scan.args.iter_cb = this;
      context.op.scan.args.iter_seek = true;
      context.op.scan.args.child_index = 0;
      auto child = &children_[context.op.scan.args.child_index];
      child->SeekToFirstAsync(context);
    } else {
      context.op.scan.args.merging_iter_cb->SeekDone(context);
    }
  }

  void SeekToFirst() override {
    ClearHeaps();
    status_ = Status::OK();
    for (auto& child : children_) {
      child.SeekToFirst();
      AddToMinHeapOrCheckStatus(&child);
    }
    direction_ = kForward;
    current_ = CurrentForward();
  }

  void SeekToLast() override {
    ClearHeaps();
    InitMaxHeap();
    status_ = Status::OK();
    for (auto& child : children_) {
      child.SeekToLast();
      AddToMaxHeapOrCheckStatus(&child);
    }
    direction_ = kReverse;
    current_ = CurrentReverse();
  }

  void SeekDone(AsyncContext& context) override {
    auto child = &children_[context.op.scan.args.child_index];
    child->Update();
    PERF_COUNTER_ADD(seek_child_seek_count, 1);
    {
      // Strictly, we timed slightly more than min heap operation,
      // but these operations are very cheap.
      PERF_TIMER_GUARD(seek_min_heap_time);
      AddToMinHeapOrCheckStatus(child);
    }
    context.op.scan.args.child_index++;
    if (context.op.scan.args.child_index < children_.size()) {
      child = &children_[context.op.scan.args.child_index];
      if (context.read.key_info.internal_key.empty()) {
        child->SeekToFirstAsync(context);
      } else {
        child->SeekAsync(context);
      }
    } else { // all child has been seeked
      direction_ = kForward;
      {
        PERF_TIMER_GUARD(seek_min_heap_time);
        current_ = CurrentForward();
      }
      context.op.scan.args.merging_iter_cb->SeekDone(context);
    }
  }

  // method call this should specify scan.merging_iter_cb
  void SeekAsync(AsyncContext& context) override {
    if (children_.size() > 0) {
      ClearHeaps();
      context.status = Status::OK();
      context.op.scan.args.iter_cb = this;
      context.op.scan.args.iter_seek = true;
      context.op.scan.args.child_index = 0;
      auto child = &children_[context.op.scan.args.child_index];
      child->SeekAsync(context);
    } else {
      context.op.scan.args.merging_iter_cb->SeekDone(context);
    }
  }

  void Seek(const Slice& target) override {
    ClearHeaps();
    status_ = Status::OK();
    for (auto& child : children_) {
      {
        PERF_TIMER_GUARD(seek_child_seek_time);
        child.Seek(target);
      }

      PERF_COUNTER_ADD(seek_child_seek_count, 1);
      {
        // Strictly, we timed slightly more than min heap operation,
        // but these operations are very cheap.
        PERF_TIMER_GUARD(seek_min_heap_time);
        AddToMinHeapOrCheckStatus(&child);
      }
    }
    direction_ = kForward;
    {
      PERF_TIMER_GUARD(seek_min_heap_time);
      current_ = CurrentForward();
    }
  }

  void SeekForPrev(const Slice& target) override {
    ClearHeaps();
    InitMaxHeap();
    status_ = Status::OK();

    for (auto& child : children_) {
      {
        PERF_TIMER_GUARD(seek_child_seek_time);
        child.SeekForPrev(target);
      }
      PERF_COUNTER_ADD(seek_child_seek_count, 1);

      {
        PERF_TIMER_GUARD(seek_max_heap_time);
        AddToMaxHeapOrCheckStatus(&child);
      }
    }
    direction_ = kReverse;
    {
      PERF_TIMER_GUARD(seek_max_heap_time);
      current_ = CurrentReverse();
    }
  }

  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current children since current_ is
    // the smallest child and key() == current_->key().
    if (direction_ != kForward) {
      SwitchToForward();
      // The loop advanced all non-current children to be > key() so current_
      // should still be strictly the smallest key.
      assert(current_ == CurrentForward());
    }

    // For the heap modifications below to be correct, current_ must be the
    // current top of the heap.
    assert(current_ == CurrentForward());

    // as the current points to the current record. move the iterator forward.
    current_->Next();
    if (current_->Valid()) {
      // current is still valid after the Next() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      assert(current_->status().ok());
      minHeap_.replace_top(current_);
    } else {
      // current stopped being valid, remove it from the heap.
      considerStatus(current_->status());
      minHeap_.pop();
    }
    current_ = CurrentForward();
  }

  void NextDone(AsyncContext& context) override {
    current_->Update();
    if (current_->Valid()) {
      assert(current_->status().ok());
      minHeap_.replace_top(current_);
    } else {
      considerStatus(current_->status());
      minHeap_.pop();
    }
    current_ = CurrentForward();
    context.op.scan.args.merging_iter_cb->NextDone(context);
  }

  void NextAsync(AsyncContext& context) override {
    assert(Valid());
    if (direction_ != kForward) { // TODO chenxu14 async this
      SwitchToForward();
      assert(current_ == CurrentForward());
    }
    assert(current_ == CurrentForward());
    context.op.scan.args.iter_cb = this;
    context.op.scan.args.iter_seek = false;
    current_->NextAsync(context);
  }

  bool NextAndGetResult(IterateResult* result) override {
    Next();
    bool is_valid = Valid();
    if (is_valid) {
      result->key = key();
      result->may_be_out_of_upper_bound = MayBeOutOfUpperBound();
    }
    return is_valid;
  }

  void Prev() override {
    assert(Valid());
    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current children since current_ is
    // the largest child and key() == current_->key().
    if (direction_ != kReverse) {
      // Otherwise, retreat the non-current children.  We retreat current_
      // just after the if-block.
      SwitchToBackward();
    }

    // For the heap modifications below to be correct, current_ must be the
    // current top of the heap.
    assert(current_ == CurrentReverse());

    current_->Prev();
    if (current_->Valid()) {
      // current is still valid after the Prev() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      assert(current_->status().ok());
      maxHeap_->replace_top(current_);
    } else {
      // current stopped being valid, remove it from the heap.
      considerStatus(current_->status());
      maxHeap_->pop();
    }
    current_ = CurrentReverse();
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  // Here we simply relay MayBeOutOfLowerBound/MayBeOutOfUpperBound result
  // from current child iterator. Potentially as long as one of child iterator
  // report out of bound is not possible, we know current key is within bound.

  bool MayBeOutOfLowerBound() override {
    assert(Valid());
    return current_->MayBeOutOfLowerBound();
  }

  bool MayBeOutOfUpperBound() override {
    assert(Valid());
    return current_->MayBeOutOfUpperBound();
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    for (auto& child : children_) {
      child.SetPinnedItersMgr(pinned_iters_mgr);
    }
  }

  bool IsKeyPinned() const override {
    assert(Valid());
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           current_->IsKeyPinned();
  }

  bool IsValuePinned() const override {
    assert(Valid());
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           current_->IsValuePinned();
  }

 private:
  // Clears heaps for both directions, used when changing direction or seeking
  void ClearHeaps();
  // Ensures that maxHeap_ is initialized when starting to go in the reverse
  // direction
  void InitMaxHeap();

  bool is_arena_mode_;
  const InternalKeyComparator* comparator_;
  autovector<IteratorWrapper, kNumIterReserve> children_;

  // Cached pointer to child iterator with the current key, or nullptr if no
  // child iterators are valid.  This is the top of minHeap_ or maxHeap_
  // depending on the direction.
  IteratorWrapper* current_;
  // If any of the children have non-ok status, this is one of them.
  Status status_;
  // Which direction is the iterator moving?
  enum Direction {
    kForward,
    kReverse
  };
  Direction direction_;
  MergerMinIterHeap minHeap_;
  bool prefix_seek_mode_;

  // Max heap is used for reverse iteration, which is way less common than
  // forward.  Lazily initialize it to save memory.
  std::unique_ptr<MergerMaxIterHeap> maxHeap_;
  PinnedIteratorsManager* pinned_iters_mgr_;

  // In forward direction, process a child that is not in the min heap.
  // If valid, add to the min heap. Otherwise, check status.
  void AddToMinHeapOrCheckStatus(IteratorWrapper*);

  // In backward direction, process a child that is not in the max heap.
  // If valid, add to the min heap. Otherwise, check status.
  void AddToMaxHeapOrCheckStatus(IteratorWrapper*);

  void SwitchToForward();

  // Switch the direction from forward to backward without changing the
  // position. Iterator should still be valid.
  void SwitchToBackward();

  IteratorWrapper* CurrentForward() const {
    assert(direction_ == kForward);
    return !minHeap_.empty() ? minHeap_.top() : nullptr;
  }

  IteratorWrapper* CurrentReverse() const {
    assert(direction_ == kReverse);
    assert(maxHeap_);
    return !maxHeap_->empty() ? maxHeap_->top() : nullptr;
  }
};

// A builder class to build a merging iterator by adding iterators one by one.
class MergeIteratorBuilder {
 public:
  // comparator: the comparator used in merging comparator
  // arena: where the merging iterator needs to be allocated from.
  explicit MergeIteratorBuilder(const InternalKeyComparator* comparator,
                                Arena* arena, bool prefix_seek_mode = false);
  ~MergeIteratorBuilder();

  // Add iter to the merging iterator.
  void AddIterator(InternalIterator* iter);

  // Get arena used to build the merging iterator. It is called one a child
  // iterator needs to be allocated.
  Arena* GetArena() { return arena; }

  // Return the result merging iterator.
  InternalIterator* Finish();

  MergingIterator* GetMergeIter() { return merge_iter; }

 private:
  MergingIterator* merge_iter;
  InternalIterator* first_iter;
  bool use_merging_iter;
  Arena* arena;
};

}  // namespace rocksdb
