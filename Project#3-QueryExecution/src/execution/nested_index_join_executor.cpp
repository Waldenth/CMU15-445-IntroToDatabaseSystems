//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/nested_index_join_executor.h"

#include <algorithm>
#include <utility>
#include <vector>

namespace bustub {

// lab3 task2 modify
NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  inner_table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  inner_index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), inner_table_info_->name_);
}

// lab3 task2 modify
void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

// lab3 task2 modify
bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple left_tuple;
  RID left_rid;
  Tuple right_raw_tuple;

  // fetch next qualified left tuple and right tuple pair
  // 获取下一个符合条件的的 (左元组,右元组) pair
  // 循环终止条件: Probe探测成功 且 谓词为空(无条件约束)或者满足谓词条件
  do {
    if (!child_executor_->Next(&left_tuple, &left_rid)) {
      return false;
    }
  } while (!Probe(&left_tuple, &right_raw_tuple) ||
           (plan_->Predicate() != nullptr &&
            !plan_->Predicate()
                 ->EvaluateJoin(&left_tuple, plan_->OuterTableSchema(), &right_raw_tuple, &(inner_table_info_->schema_))
                 .GetAs<bool>()));
  // lock on to-read left and right rid
  // ...
  // ...

  // 生成输出元组
  std::vector<Value> values;
  std::transform(plan_->OutputSchema()->GetColumns().begin(), plan_->OutputSchema()->GetColumns().end(),
                 std::back_inserter(values),
                 [&left_tuple = left_tuple, &right_raw_tuple, &plan = plan_,
                  &inner_table_schema = inner_table_info_->schema_](const Column &col) {
                   return col.GetExpr()->EvaluateJoin(&left_tuple, plan->OuterTableSchema(), &right_raw_tuple,
                                                      &inner_table_schema);
                 });

  *tuple = Tuple(values, plan_->OutputSchema());

  return true;
}

// 利用innerTable的索引查找符合 JOIN 条件的 right_tuple 存入right_raw_tuple
bool NestIndexJoinExecutor::Probe(Tuple *left_tuple, Tuple *right_raw_tuple) {
  Value key_value = plan_->Predicate()->GetChildAt(0)->EvaluateJoin(left_tuple, plan_->OuterTableSchema(),
                                                                    right_raw_tuple, &(inner_table_info_->schema_));
  Tuple probe_key = Tuple{{key_value}, inner_index_info_->index_->GetKeySchema()};

  std::vector<RID> result_set;
  GetBPlusTreeIndex()->ScanKey(probe_key, &result_set, exec_ctx_->GetTransaction());

  if (result_set.empty()) {
    return false;
  }

  return inner_table_info_->table_->GetTuple(result_set[0], right_raw_tuple, exec_ctx_->GetTransaction());
}

}  // namespace bustub
