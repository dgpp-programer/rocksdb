//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Introduction of SyncPoint effectively disabled building and running this test
// in Release build.
// which is a pity, it is a good test
#include <fcntl.h>
#include <algorithm>
#include <set>
#include <thread>
#include <unordered_set>
#include <utility>
#ifndef OS_WIN
#include <unistd.h>
#endif
#ifdef OS_SOLARIS
#include <alloca.h>
#endif

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "env/mock_env.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_factory.h"
#include "test_util/testutil.h"

namespace rocksdb {

class DBAsyncTest : public DBTestBase {
 public:
  DBAsyncTest() : DBTestBase("/db_async_test") {}
};

void TestWithOptions(Options& options) {
  DB* db;
  const Slice keys[] = {Slice("aaa"), Slice("bbb"), Slice("ccc")};
  const Slice vals[] = {Slice("foo"), Slice("bar"), Slice("baz")};
  ASSERT_OK(DB::Open(options, "/dir/db", &db));
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_OK(db->Put(WriteOptions(), keys[i], vals[i]));
  }

  struct AsyncContext *ctx = reinterpret_cast<AsyncContext*>(
      calloc(1, sizeof(struct AsyncContext)));
  PinnableSlice pinnable_val;
  if (!ctx) {
    return;
  }
  ReadOptions read_opts;
  ctx->options = &read_opts;
  ctx->get.cf = db->DefaultColumnFamily();
  ctx->get.value = &pinnable_val;
  ctx->get.callback = [&](AsyncContext& ctx_) {
    ASSERT_OK(ctx_.status);
    size_t index = ctx->get.start_time;
    ASSERT_TRUE(*ctx_.get.value == vals[index]);
    ctx_.get.value->Reset();
  };

  // query memtable
  for (size_t i = 0; i < 3; ++i) {
    // test async read is OK
    ctx->get.key = const_cast<Slice*>(&keys[i]);
    ctx->get.start_time = i;
    db->GetAsync(*ctx);
    // test sync read is OK too
    std::string res;
    ASSERT_OK(db->Get(ReadOptions(), keys[i], &res));
    ASSERT_TRUE(res == vals[i]);
  }
  Iterator* iterator = db->NewIterator(ReadOptions());
  iterator->SeekToFirst();
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_TRUE(iterator->Valid());
    ASSERT_TRUE(keys[i] == iterator->key());
    ASSERT_TRUE(vals[i] == iterator->value());
    iterator->Next();
  }
  ASSERT_TRUE(!iterator->Valid());
  delete iterator;

  // TEST_FlushMemTable() is not supported in ROCKSDB_LITE
#ifndef ROCKSDB_LITE
  DBImpl* dbi = reinterpret_cast<DBImpl*>(db);
  ASSERT_OK(dbi->TEST_FlushMemTable());
  // query sstfile
  for (size_t i = 0; i < 3; ++i) {
    // test async read is OK
    ctx->get.key = const_cast<Slice*>(&keys[i]);
    ctx->get.start_time = i;
    db->GetAsync(*ctx);
    // test sync read is OK too
    std::string res;
    ASSERT_OK(db->Get(ReadOptions(), keys[i], &res));
    ASSERT_TRUE(res == vals[i]);
  }
#endif  // ROCKSDB_LITE
  free(ctx);
  delete db;
}

TEST_F(DBAsyncTest, DefaultIndexAndFilterTest) {
  std::unique_ptr<MockEnv> env{new MockEnv(Env::Default())};
  DBOptions dbOpts;
  ColumnFamilyOptions cfOpts;
  BlockBasedTableOptions bbtOpts;
  Options options = Options(dbOpts, cfOpts);
  bbtOpts.no_block_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbtOpts));
  options.create_if_missing = true;
  options.env = env.get();
  TestWithOptions(options);
}

TEST_F(DBAsyncTest, PartitionIndexAndFilterTest) {
  std::unique_ptr<MockEnv> env{new MockEnv(Env::Default())};
  DBOptions dbOpts;
  ColumnFamilyOptions cfOpts;
  Options options = Options(dbOpts, cfOpts);

  BlockBasedTableOptions bbtOpts;
  bbtOpts.no_block_cache = true;
  bbtOpts.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbtOpts.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  bbtOpts.partition_filters = true;

  options.table_factory.reset(NewBlockBasedTableFactory(bbtOpts));
  options.create_if_missing = true;
  options.env = env.get();
  TestWithOptions(options);
}

}  // namespace rocksdb

#ifdef ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS
extern "C" {
void RegisterCustomObjects(int argc, char** argv);
}
#else
void RegisterCustomObjects(int /*argc*/, char** /*argv*/) {}
#endif  // !ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
