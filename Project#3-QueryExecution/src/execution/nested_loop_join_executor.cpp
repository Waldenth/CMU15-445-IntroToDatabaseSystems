//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

// lab3 task2 modify
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      left_executor_{std::move(left_executor)},
      right_executor_{std::move(right_executor)} {}

// lab3 task2 modify
void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
}

// lab3 task2 modify
bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  RID left_rid;
  // check if left tuple is initialized

  // 如果JOIN算子的left_tuple空,先调用左子算子(Outer Table)Next获得left_tuple,left_rid
  if (left_tuple.GetLength() == 0 && !left_executor_->Next(&left_tuple, &left_rid)) {
    return false;
  }

  // lock on to-read left rid
  // ...
  // ...

  // fetch next qualified left tuple and right tuple pair
  Tuple right_tuple;
  RID right_rid;

  // 将当前的OuterTable.tuple(left_tuple)与each right_tuple进行谓词匹配计算
  // 直到之后第一个符合条件的right_tuple, right_rid
  do {
    // 循环调用右子算子(Inner Table)Next,获得 next right_tuple, right_rid(下一条tuple)
    if (!Advance(&left_rid, &right_tuple, &right_rid)) {
      return false;
    }
  } while (plan_->Predicate() != nullptr && !plan_->Predicate()
                                                 ->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(),
                                                                &right_tuple, right_executor_->GetOutputSchema())
                                                 .GetAs<bool>());

  // lock on to-read rid
  // ...
  // ...

  // 生成输出tuple
  std::vector<Value> values;
  std::transform(plan_->OutputSchema()->GetColumns().begin(), plan_->OutputSchema()->GetColumns().end(),
                 std::back_inserter(values),
                 [&left_tuple = left_tuple, &left_executor = left_executor_, &right_tuple,
                  &right_executor = right_executor_](const Column &col) {
                   return col.GetExpr()->EvaluateJoin(&left_tuple, left_executor->GetOutputSchema(), &right_tuple,
                                                      right_executor->GetOutputSchema());
                 });

  *tuple = Tuple(values, plan_->OutputSchema());

  return true;
}

bool NestedLoopJoinExecutor::Advance(RID *left_rid, Tuple *right_tuple, RID *right_rid) {
  if (!right_executor_->Next(right_tuple, right_rid)) {
    // 右子算子Next调用失败(tuple遍历到末尾了), 调用左子算子获取  next left_tuple left_rid
    if (!left_executor_->Next(&left_tuple, left_rid)) {
      return false;
    }
    // 再初始化右子算子, 进行Next调用(从头开始遍历 右 tuple)
    right_executor_->Init();
    right_executor_->Next(right_tuple, right_rid);
  }
  return true;
}

}  // namespace bustub
