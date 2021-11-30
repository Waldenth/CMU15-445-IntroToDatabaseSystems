//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/insert_executor.h"

#include <algorithm>
#include <memory>

namespace bustub {

// lab3 task2 modify
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

// lab3 task2 modify
void InsertExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }

  table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

// lab3 task2 modify
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple to_insert_tuple;

  // 如果将insert值直接嵌入到计划中，则为true；如果是child plan提供tuple，则为false
  if (plan_->IsRawInsert()) {
    if (next_insert >= plan_->RawValues().size()) {
      return false;
    }
    to_insert_tuple = Tuple(plan_->RawValuesAt(next_insert), &(table_info_->schema_));
    ++next_insert;
  } else {
    RID emit_rid;
    if (!child_executor_->Next(&to_insert_tuple, &emit_rid)) {
      return false;
    }
  }

  bool inserted = table_info_->table_->InsertTuple(to_insert_tuple, rid, exec_ctx_->GetTransaction());

  if (inserted) {
    // lock on new rid
    // exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid);

    // maintain each index on inserted tuple
    std::for_each(table_indexes.begin(), table_indexes.end(),
                  [&to_insert_tuple, &rid, &table_info = table_info_, &exec_ctx = exec_ctx_](IndexInfo *index) {
                    index->index_->InsertEntry(to_insert_tuple.KeyFromTuple(table_info->schema_, index->key_schema_,
                                                                            index->index_->GetKeyAttrs()),
                                               *rid, exec_ctx->GetTransaction());
                    // 添加一个新的IndexWriteRecord到list of index write records of this transaction队尾
                    // IndexWriteRecord由emplace_back参数直接构造(push_back无这个功能)
                    exec_ctx->GetTransaction()->GetIndexWriteSet()->emplace_back(
                        *rid, table_info->oid_, WType::INSERT, to_insert_tuple, Tuple{}, index->index_oid_,
                        exec_ctx->GetCatalog());
                  });
  }

  return inserted;
}

}  // namespace bustub
