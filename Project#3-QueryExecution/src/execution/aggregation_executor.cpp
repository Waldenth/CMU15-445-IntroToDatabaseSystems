//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/aggregation_executor.h"

#include <memory>
#include <vector>

namespace bustub {

// lab3 task2 modify
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      child_{std::move(child)},
      aht_{plan_->GetAggregates(), plan_->GetAggregateTypes()},
      aht_iterator_{aht_.Begin()} {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

// lab3 tesk2 modify
void AggregationExecutor::Init() {
  child_->Init();

  Tuple tuple;
  RID rid;

  while (child_->Next(&tuple, &rid)) {
    // lock on to-read rid
    // ...
    // ...

    aht_.InsertCombine(MakeKey(&tuple), MakeVal(&tuple));
  }

  aht_iterator_ = aht_.Begin();
}

// lab3 task2 modify
bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  // fetch qualified group_bys and aggregates
  std::vector<Value> group_bys;
  std::vector<Value> aggregates;

  do {
    if (aht_iterator_ == aht_.End()) {
      return false;
    }

    group_bys = aht_iterator_.Key().group_bys_;
    aggregates = aht_iterator_.Val().aggregates_;

    ++aht_iterator_;

  } while (plan_->GetHaving() != nullptr &&
           !plan_->GetHaving()->EvaluateAggregate(group_bys, aggregates).GetAs<bool>());

  // 生成输出元组
  std::vector<Value> values;

  std::transform(
      plan_->OutputSchema()->GetColumns().begin(), plan_->OutputSchema()->GetColumns().end(),
      std::back_inserter(values),
      [&group_bys, &aggregates](const Column &col) { return col.GetExpr()->EvaluateAggregate(group_bys, aggregates); }

  );

  *tuple = Tuple{values, plan_->OutputSchema()};

  return true;
}

}  // namespace bustub
