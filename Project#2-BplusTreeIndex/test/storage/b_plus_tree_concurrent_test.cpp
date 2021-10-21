/**
 * b_plus_tree_test.cpp
 */

#include <algorithm>
#include <chrono>  // NOLINT
#include <cstdio>
#include <functional>
#include <iterator>
#include <numeric>
#include <random>
#include <thread>                   // NOLINT
#include "b_plus_tree_test_util.h"  // NOLINT

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {
// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&... args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);
  }
  delete transaction;
}

// helper function to separate insert
void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

// helper function to insert and get
void InsertAndGetHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                        __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);

    std::vector<RID> rids;
    bool got = tree->GetValue(index_key, &rids, transaction);
    EXPECT_EQ(got, true);
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  delete transaction;
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &remove_keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree->Remove(index_key, transaction);
  }
  delete transaction;
}

// helper function to separate delete
void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                       const std::vector<int64_t> &remove_keys, int total_threads,
                       __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree->Remove(index_key, transaction);
    }
  }
  delete transaction;
}

void DeleteAndGetHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                        const std::vector<int64_t> &remove_keys, __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree->Remove(index_key, transaction);
    std::vector<RID> rids;
    bool got = tree->GetValue(index_key, &rids, transaction);
    EXPECT_EQ(got, false);
    EXPECT_EQ(rids.size(), 0);
  }
  delete transaction;
}

TEST(BPlusTreeConcurrentTest, InsertTest_Simple) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, InsertTest_Simple_With_Separate) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelperSplit, &tree, keys, 2);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, InsertTest_With_Split) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 6;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    // EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, InsertTest_Many) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, InsertTest_Many_With_Random) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 10, 10);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 1000;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(keys.begin(), keys.end(), g);
  LaunchParallelTest(2, InsertAndGetHelper, &tree, keys);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, InsertTest_Many_With_Random_With_Separate) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 10, 10);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 1000;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(keys.begin(), keys.end(), g);

  LaunchParallelTest(4, InsertHelperSplit, &tree, keys, 4);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, InsertTest_Many_With_Separate) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelperSplit, &tree, keys, 2);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest_Simple1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest_Simple1_With_Separate) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
  LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

  int64_t start_key = 7;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 4);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest_Simple2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 5, 6, 7, 8, 3, 4};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {8, 1, 2, 7};
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 3;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 4);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest_Simple2_With_Separate) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 5, 6, 7, 8, 3, 4};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {8, 1, 2, 7};
  LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

  int64_t start_key = 3;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 4);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest_With_Coalesce) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 5, 6, 7, 8, 3, 4};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 2, 8, 7, 3};
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 4;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest_With_Coalesce_With_Separate) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 5, 6, 7, 8, 3, 4};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {8, 1, 2, 7, 6};
  LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

  int64_t start_key = 3;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest_With_Redistribute) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 5, 6, 7, 8, 3, 4};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 2, 3};
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 4;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 5);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest_With_Redistribute2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 5, 6, 9, 10, 11, 3, 4, 12, 7, 8};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 2, 3, 6, 7};
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 4;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key == 5 ? 8 : current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 7);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest_With_Redistribute2_With_Separate) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 5, 6, 9, 10, 11, 3, 4, 12, 7, 8};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 2, 3, 6, 7};
  LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

  int64_t start_key = 4;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key == 5 ? 8 : current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 7);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest_Complex1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys;
  // remove all odd keys
  for (int64_t key = 1; key < scale_factor; key += 2) {
    remove_keys.push_back(key);
  }
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 2;
    size = size + 1;
  }

  EXPECT_EQ(size, 49);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest_Complex_With_Separate) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys;
  // remove all odd keys
  for (int64_t key = 1; key < scale_factor; key += 2) {
    remove_keys.push_back(key);
  }
  LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

  int64_t start_key = 2;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 2;
    size = size + 1;
  }

  EXPECT_EQ(size, 49);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, MixTest_Simple1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // first, populate index
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  // concurrent insert
  keys.clear();
  for (int i = 6; i <= 10; i++) {
    keys.push_back(i);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);
  // concurrent delete
  std::vector<int64_t> remove_keys = {1, 4, 3, 5, 6};
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key == 2 ? 7 : current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 5);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, MixTest_Simple2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // concurrent insert
  std::vector<int64_t> keys;
  for (int i = 1; i <= 20; i++) {
    keys.push_back(i);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);

  // concurrent delete
  std::vector<int64_t> remove_keys = {1, 10, 11, 12, 13, 14, 15};
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key == 9 ? 16 : current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 13);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, MixTest_Simple3) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // first, populate index
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  // concurrent insert
  keys.clear();
  for (int i = 6; i <= 100; i++) {
    keys.push_back(i);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);

  // concurrent delete
  std::vector<int64_t> remove_keys = {1, 99, 49, 11, 25};
  for (int i = 50; i <= 100; i++) {
    remove_keys.push_back(i);
  }
  for (int i = 1; i <= 100; i += 2) {
    remove_keys.push_back(i);
  }
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 2;
    size = size + 1;
  }

  EXPECT_EQ(size, 24);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, MixTest_Simple4) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // first, populate index
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  // concurrent insert
  keys.clear();
  for (int i = 6; i <= 50; i++) {
    keys.push_back(i);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);

  keys.clear();
  for (int i = 51; i <= 70; i++) {
    keys.push_back(i);
  }
  LaunchParallelTest(2, InsertHelperSplit, &tree, keys, 2);

  keys.clear();
  for (int i = 71; i <= 100; i++) {
    keys.push_back(i);
  }
  LaunchParallelTest(3, InsertHelperSplit, &tree, keys, 3);

  // concurrent delete
  std::vector<int64_t> remove_keys = {1, 99, 49, 11, 25, 70};
  LaunchParallelTest(3, DeleteHelperSplit, &tree, remove_keys, 3);

  for (int i = 50; i <= 100; i++) {
    remove_keys.push_back(i);
  }
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  for (int i = 1; i <= 100; i += 2) {
    remove_keys.push_back(i);
  }
  LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

  int64_t start_key = 2;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 2;
    size = size + 1;
  }

  EXPECT_EQ(size, 24);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, MixTest_Complex1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // concurrently insert keys and delete keys
  std::vector<int64_t> delete_keys{2, 5, 8, 4, 3, 6, 7, 1};
  std::vector<int64_t> remain_keys{13, 10, 16, 9, 11, 12, 14, 15};

  LaunchParallelTest(2, InsertHelperSplit, &tree, delete_keys, 2);

  std::thread t0(InsertHelper, &tree, remain_keys, 0);
  LaunchParallelTest(1, DeleteHelper, &tree, delete_keys);
  t0.join();

  std::vector<RID> rids;
  for (auto key : delete_keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    auto got = tree.GetValue(index_key, &rids);
    EXPECT_EQ(false, got);
  }

  int64_t current_key = 9;
  int64_t size = 0;
  index_key.SetFromInteger(current_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 8);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, MixTest_Complex2_With_Random) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 21, 21);
  GenericKey<8> index_key;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // concurrently insert keys and delete keys
  int64_t scale_factor = 1000;

  std::vector<int64_t> remain_keys(scale_factor);
  std::vector<int64_t> delete_keys(scale_factor);
  std::iota(delete_keys.begin(), delete_keys.end(), 1);
  std::iota(remain_keys.begin(), remain_keys.end(), scale_factor + 1);

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(delete_keys.begin(), delete_keys.end(), g);
  std::shuffle(remain_keys.begin(), remain_keys.end(), g);

  LaunchParallelTest(2, InsertHelperSplit, &tree, delete_keys, 2);

  std::thread t0(InsertHelper, &tree, remain_keys, 0);
  LaunchParallelTest(1, DeleteHelper, &tree, delete_keys);
  t0.join();

  std::vector<RID> rids;
  for (auto key : delete_keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    auto got = tree.GetValue(index_key, &rids);
    EXPECT_EQ(false, got);
  }

  int64_t current_key = scale_factor + 1;
  int64_t size = 0;
  index_key.SetFromInteger(current_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end() && size < scale_factor + 1; ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, scale_factor);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, MixTest_Complex3_With_Random_With_More_Threads) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 100, 100);
  GenericKey<8> index_key;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  int64_t scale_factor = 10000;
  std::vector<int64_t> all_keys(scale_factor);
  std::iota(all_keys.begin(), all_keys.end(), 1);
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(all_keys.begin(), all_keys.end(), g);

  int64_t interval = scale_factor / 4;
  std::vector<int64_t> remain_keys1(std::next(all_keys.begin(), interval * 0),
                                    std::next(all_keys.begin(), interval * 1));
  std::vector<int64_t> remain_keys2(std::next(all_keys.begin(), interval * 1),
                                    std::next(all_keys.begin(), interval * 2));
  std::vector<int64_t> delete_keys1(std::next(all_keys.begin(), interval * 2),
                                    std::next(all_keys.begin(), interval * 3));
  std::vector<int64_t> delete_keys2(std::next(all_keys.begin(), interval * 3),
                                    std::next(all_keys.begin(), interval * 4));

  LaunchParallelTest(4, InsertHelperSplit, &tree, delete_keys1, 4);
  LaunchParallelTest(4, InsertHelperSplit, &tree, delete_keys2, 4);

  // concurrent insert and delete
  std::thread t0(InsertAndGetHelper, &tree, remain_keys1, 0);
  std::thread t1(InsertAndGetHelper, &tree, remain_keys2, 1);
  std::thread t2(DeleteAndGetHelper, &tree, delete_keys1, 2);
  std::thread t3(DeleteAndGetHelper, &tree, delete_keys2, 3);

  t0.join();
  t1.join();
  t2.join();
  t3.join();

  std::vector<RID> rids;
  for (auto key : delete_keys1) {
    rids.clear();
    index_key.SetFromInteger(key);
    auto got = tree.GetValue(index_key, &rids);
    EXPECT_EQ(false, got);
  }
  for (auto key : delete_keys2) {
    rids.clear();
    index_key.SetFromInteger(key);
    auto got = tree.GetValue(index_key, &rids);
    EXPECT_EQ(false, got);
  }

  int64_t current_key = 0;
  int64_t size = 0;
  index_key.SetFromInteger(current_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    size = size + 1;
  }

  EXPECT_EQ(size, scale_factor / 2);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

}  // namespace bustub
