//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexJoinExecutor executes index join operations.
 */
class NestIndexJoinExecutor : public AbstractExecutor {
  using KeyType = GenericKey<8>;
  using ValueType = RID;
  using KeyComparator = GenericComparator<8>;

 public:
  /**
   * Creates a new nested index join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the nested index join plan node
   * @param outer table child
   */
  NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                        std::unique_ptr<AbstractExecutor> &&child_executor);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

 private:
  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;

  // lab3 task2 add
  /** Metadata identifying the inner table that should be fetched. */
  const TableMetadata *inner_table_info_;
  /** Index info identifying the index of the inner table to be probed */
  /** 标识了要探测的innerTable的索引 的索引信息 */
  const IndexInfo *inner_index_info_;

  std::unique_ptr<AbstractExecutor> child_executor_;

  BPlusTreeIndex<KeyType, ValueType, KeyComparator> *GetBPlusTreeIndex() {
    return dynamic_cast<BPlusTreeIndex<KeyType, ValueType, KeyComparator> *>(inner_index_info_->index_.get());
  }

  bool Probe(Tuple *left_tuple, Tuple *right_raw_tuple);
};
}  // namespace bustub
