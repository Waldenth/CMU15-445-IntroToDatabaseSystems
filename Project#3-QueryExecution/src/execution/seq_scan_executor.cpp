//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

#include <algorithm>
#include <vector>

namespace bustub {

// lab3 task2 modify
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_{plan} {
  // exec_ctx => ExecutorContext
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}
// lab3 task2 modify
void SeqScanExecutor::Init() {
  table_iter = std::make_unique<TableIterator>(table_info_->table_->Begin(exec_ctx_->GetTransaction()));
}
// lab3 task2 modify
bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // fetch raw tuple from table
  Tuple raw_tuple;

  // 利用迭代器顺序扫描table,直到SeqScanExecutor::table_iter指向遇到的第一个满足谓词条件的tuple
  do {
    if (*table_iter == table_info_->table_->End()) {
      return false;
    }
    // TableIterator重载了运算符*,获取TableIterator.tuple_,即迭代器指向的table.tuple
    raw_tuple = *(*table_iter);

    ++(*table_iter);
  } while (plan_->GetPredicate() != nullptr &&
           !plan_->GetPredicate()->Evaluate(&raw_tuple, &(table_info_->schema_)).GetAs<bool>());
  // GetPredicate() 获取测试元组的谓词(判断是否存在满足某种条件的记录，存在返回TRUE、不存在返回FALSE),SQL eg:
  // LIKE,IN,NOT NULL,EXISTS..
  // Evaluate() 通过使用给定schema计算tuple而获得值,即判断raw_tuple是否满足table_info_指出的模式

  // lock on to-read RID
  // ...
  // ...

  // 生成输出tuple
  /**
   * @brief Value是一个抽象类，表示存储在中的SQL数据的视图
   * 一些物质化状态。所有Value都有一个类型和比较函数，
   * 但子类实现其他特定于类型的功能。
   */
  std::vector<Value> values;

  std::transform(plan_->OutputSchema()->GetColumns().begin(),
                 plan_->OutputSchema()->GetColumns().end(),  // range
                 std::back_inserter(values),                 // tranform storager
                 [&raw_tuple, &table_info = table_info_](const Column &col) {
                   // 返回通过使用给定schema计算tuple而获得的value
                   return col.GetExpr()->Evaluate(&raw_tuple, &(table_info->schema_));
                 });
  *tuple = Tuple{values, plan_->OutputSchema()};
  *rid = raw_tuple.GetRid();

  return true;
}

}  // namespace bustub
