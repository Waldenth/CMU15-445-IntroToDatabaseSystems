#pragma once

#include <algorithm>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/index.h"
#include "storage/table/table_heap.h"

namespace bustub {

/**
 * Typedefs
 */
using table_oid_t = uint32_t;
using column_oid_t = uint32_t;
using index_oid_t = uint32_t;

/**
 * Metadata about a table.
 */
struct TableMetadata {
  TableMetadata(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
      : schema_(std::move(schema)), name_(std::move(name)), table_(std::move(table)), oid_(oid) {}
  Schema schema_;
  std::string name_;
  std::unique_ptr<TableHeap> table_;
  table_oid_t oid_;
};

/**
 * Metadata about a index
 */
struct IndexInfo {
  IndexInfo(Schema key_schema, std::string name, std::unique_ptr<Index> &&index, index_oid_t index_oid,
            std::string table_name, size_t key_size)
      : key_schema_(std::move(key_schema)),
        name_(std::move(name)),
        index_(std::move(index)),
        index_oid_(index_oid),
        table_name_(std::move(table_name)),
        key_size_(key_size) {}
  Schema key_schema_;
  std::string name_;
  std::unique_ptr<Index> index_;
  index_oid_t index_oid_;
  std::string table_name_;
  const size_t key_size_;
};

/**
 * Catalog is a non-persistent catalog that is designed for the executor to use.
 * It handles table creation and table lookup.
 */
class Catalog {
 public:
  /**
   * Creates a new catalog object.
   * @param bpm the buffer pool manager backing tables created by this catalog
   * @param lock_manager the lock manager in use by the system
   * @param log_manager the log manager in use by the system
   */
  Catalog(BufferPoolManager *bpm, LockManager *lock_manager, LogManager *log_manager)
      : bpm_{bpm}, lock_manager_{lock_manager}, log_manager_{log_manager} {}

  /**
   * Create a new table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param table_name the name of the new table
   * @param schema the schema of the new table
   * @return a pointer to the metadata of the new table
   * lab3 实现
   */
  TableMetadata *CreateTable(Transaction *txn, const std::string &table_name, const Schema &schema) {
    BUSTUB_ASSERT(names_.count(table_name) == 0, "Table names should be unique!");

    // 构造一个由Catalog.(bpm_, lock_manager_, log_manager_),txn构造得到的TableHeap的智能指针
    std::unique_ptr<TableHeap> table_heap = std::make_unique<TableHeap>(bpm_, lock_manager_, log_manager_, txn);

    // <utility> std::move 用于指示对象 t 可以被移动
    std::unique_ptr<TableMetadata> table_meta_data =
        std::make_unique<TableMetadata>(schema, table_name, std::move(table_heap), next_table_oid_);

    // 返回指向被管理对象的指针
    TableMetadata *result = table_meta_data.get();

    // std::unordered_map<table_oid_t, std::unique_ptr<TableMetadata>>
    // 在哈希表tables_的table_oid_t位置前插入table_meta_data
    tables_.emplace(result->oid_, std::move(table_meta_data));
    names_.emplace(result->name_, result->oid_);

    next_table_oid_++;

    return result;
  }

  /** @return table metadata by name
   * lab3 实现
   */
  TableMetadata *GetTable(const std::string &table_name) {
    std::unordered_map<std::string, table_oid_t>::iterator name_it = names_.find(table_name);

    // Catalog.names_中找不到该表名的表
    if (name_it == names_.end()) {
      throw std::out_of_range("Catalog.GetTable: can not find table by table name.");
    }

    // 根据找到的bustub::table_oid_t调用下一级GetTable获得对应的TableMetadata * 指针
    return GetTable(name_it->second);
  }

  /** @return table metadata by oid
   * lab3 实现
   */
  TableMetadata *GetTable(table_oid_t table_oid) {
    std::unordered_map<table_oid_t, std::unique_ptr<TableMetadata>>::iterator table_it = tables_.find(table_oid);

    if (table_it == tables_.end()) {
      throw std::out_of_range("Catalog.GetTable: can not find table by table oid.");
    }

    return table_it->second.get();
  }

  /**
   * Create a new index, populate existing data of the table and return its metadata.
   * 创建新索引，填充表的现有数据并返回其元数据
   * @param txn the transaction in which the table is being created
   * @param index_name the name of the new index
   * @param table_name the name of the table
   * @param schema the schema of the table 表的模式
   * @param key_schema the schema of the key
   * @param key_attrs key attributes
   * @param keysize size of the key
   * @return a pointer to the metadata of the new table
   * lab3 实现
   */
  template <class KeyType, class ValueType, class KeyComparator>
  IndexInfo *CreateIndex(Transaction *txn, const std::string &index_name, const std::string &table_name,
                         const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs,
                         size_t keysize) {
    std::unique_ptr<IndexMetadata> index_meta_data =
        std::make_unique<IndexMetadata>(std::string(index_name), std::string(table_name), &schema, key_attrs);

    std::unique_ptr<Index> index = std::make_unique<BPLUSTREE_INDEX_TYPE>(index_meta_data.release(), bpm_);

    std::unique_ptr<IndexInfo> index_info = std::make_unique<IndexInfo>(
        key_schema, std::string(index_name), std::move(index), next_index_oid_, std::string(table_name), keysize);

    IndexInfo *result = index_info.get();

    indexes_.emplace(result->index_oid_, std::move(index_info));

    // 根据result->table_name_从index_names_ map数组中找到指定的std::unordered_map<std::string, index_oid_t>
    // 在指定的map中插入元素(result->name_, result->index_oid_)
    index_names_[result->table_name_].emplace(result->name_, result->index_oid_);
    next_index_oid_++;

    // 填充表的现有数据
    TableMetadata *table_meta_data = GetTable(result->table_name_);
    TableHeap *table_heap = table_meta_data->table_.get();
    for (TableIterator it = table_heap->Begin(txn); it != table_heap->End(); it++) {
      result->index_->InsertEntry(it->KeyFromTuple(schema, result->key_schema_, result->index_->GetKeyAttrs()),
                                  it->GetRid(), txn);
    }

    return result;
  }

  // lab3 实现
  IndexInfo *GetIndex(const std::string &index_name, const std::string &table_name) {
    std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>>::iterator index_name_it =
        index_names_.find(table_name);

    if (index_name_it == index_names_.end()) {
      throw std::out_of_range("GetIndex: cannot find index by table name.");
    }

    std::unordered_map<std::string, index_oid_t> inner_map = index_name_it->second;
    std::unordered_map<std::string, index_oid_t>::iterator inner_it = inner_map.find(index_name);

    if (inner_it == inner_map.end()) {
      throw std::out_of_range("GetIndex: cannot find index by index name.");
    }

    return GetIndex(inner_it->second);
  }

  // lab3 实现
  IndexInfo *GetIndex(index_oid_t index_oid) {
    std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>>::iterator index_it = indexes_.find(index_oid);

    if (index_it == indexes_.end()) {
      throw std::out_of_range("GetIndex: cannot find index by index oid.");
    }

    return index_it->second.get();
  }

  // lab3 实现
  std::vector<IndexInfo *> GetTableIndexes(const std::string &table_name) {
    std::vector<IndexInfo *> result;

    std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>>::iterator index_name_it =
        index_names_.find(table_name);

    if (index_name_it == index_names_.end()) {
      return result;
    }

    std::unordered_map<std::string, index_oid_t> inner_map = index_name_it->second;

    // 将整个范围内的inner_map的元素执行变换策略(lambda表达式)
    // 并将结果存储到result中
    // 变换策略为对inner_map中的元素std::pair<std::string, index_oid_t>
    // 在indexes_中根据index_oid_t找到对应的std::unique_ptr<IndexInfo>
    // 找到的IndexInfo *存储到result中
    std::transform(inner_map.begin(), inner_map.end(), std::back_inserter(result),
                   [&idx_map = indexes_](auto pair) { return idx_map[pair.second].get(); });

    return result;
  }

 private:
  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] LockManager *lock_manager_;
  [[maybe_unused]] LogManager *log_manager_;

  /** tables_ : table identifiers -> table metadata. Note that tables_ owns all table metadata. */
  std::unordered_map<table_oid_t, std::unique_ptr<TableMetadata>> tables_;
  /** names_ : table names -> table identifiers */
  std::unordered_map<std::string, table_oid_t> names_;
  /** The next table identifier to be used. */
  std::atomic<table_oid_t> next_table_oid_{0};
  /** indexes_: index identifiers -> index metadata. Note that indexes_ owns all index metadata */
  std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>> indexes_;
  /** index_names_: table name -> index names -> index identifiers */
  std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>> index_names_;
  /** The next index identifier to be used */
  std::atomic<index_oid_t> next_index_oid_{0};
};
}  // namespace bustub
