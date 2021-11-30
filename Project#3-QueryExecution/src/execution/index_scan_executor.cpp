//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {

// lab3 task2 modify
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_{plan} {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
}
// lab3 task2 modify
void IndexScanExecutor::Init() {
  index_iter = std::make_unique<INDEXITERATOR_TYPE>(GetBPlusTreeIndex()->GetBeginIterator());
}
// lab3 task2 modify
bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  // fetch raw tuple from table
  Tuple raw_tuple;

  do {
    if (*index_iter == GetBPlusTreeIndex()->GetEndIterator()) {
      return false;
    }

    bool fetched = table_info_->table_->GetTuple((*(*index_iter)).second, &raw_tuple, exec_ctx_->GetTransaction());

    if (!fetched) {
      return false;
    }

    ++(*index_iter);
  } while (plan_->GetPredicate() != nullptr &&
           !plan_->GetPredicate()->Evaluate(&raw_tuple, &(table_info_->schema_)).GetAs<bool>());

  // 生成输出tuple
  std::vector<Value> values;
  std::transform(plan_->OutputSchema()->GetColumns().begin(),
                 plan_->OutputSchema()->GetColumns().end(),  // range
                 std::back_inserter(values),                 // tranform storager
                 [&raw_tuple, &table_info = table_info_](const Column &col) {
                   return col.GetExpr()->Evaluate(&raw_tuple, &(table_info->schema_));
                 });

  *tuple = Tuple{values, plan_->OutputSchema()};
  *rid = raw_tuple.GetRid();

  return true;
}

}  // namespace bustub
