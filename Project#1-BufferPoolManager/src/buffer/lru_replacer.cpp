//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity{num_pages} {}

LRUReplacer::~LRUReplacer() = default;

/**
 * 根据LRU策略从LRU lst中找到牺牲页,(lst 最前面的元素是最早加进来的)
 * 1.如果没有可以牺牲的page直接返回false
 * 2.如果有的话选择在链表尾部的page, remove它即可. 这里的删除涉及链表和hash表两个数据结构的删除
 * 3.为了防止并发错误,以下操作均需要加锁
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // 在lock_guard对象构造时，传入的mutex对象会被当前线程锁住
  const std::lock_guard<mutex_t> guard(mutex);

  if (lst.empty()) {
    return false;
  }

  auto f = lst.front();
  lst.pop_front();

  // map.find()如果未找到则返回引用map.end()的迭代器
  auto hash_it = hash.find(f);

  if (hash_it != hash.end()) {  // 如果不是没有找到元素
    hash.erase(hash_it);
    *frame_id = f;
    return true;
  }

  return false;
}

/**
 * 在将某page固定到BufferPoolManager中的某一frame之后,应调用此方法Pin()
 * 此方法应该从LRUReplacer中删除包含这个已经固定的page的某一frame,表示该frame不能成为LRU牺牲的对象
 *
 * 1.pin函数表示这个frame被某个进程引用了
 * 2.被引用的frame不能成为LRU算法的牺牲目标.所以这里把它从我们的数据结构中删除
 *
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  const std::lock_guard<mutex_t> guard(mutex);

  // hash是一个unordered_map; key=frame_id_t , value=list<frame_id_t> lst的迭代器
  // hash_it尝试找到hash中Key=frame_id的键值对,返回指向该KV的迭代器
  auto hash_it = hash.find(frame_id);
  if (hash_it != hash.end()) {
    // erase()方法是删除iterator指定的节点
    lst.erase(hash_it->second);  // 从lst中删除该页 (hash_it->second是指向该页的迭代器)
    hash.erase(hash_it);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  const std::lock_guard<mutex_t> guard(mutex);

  if (hash.size() >= capacity) {
    return;
  }

  // This step of logical processing is very IMPORTANT
  // BUT I DO NOT KNOW THE REASON
  auto hash_it = hash.find(frame_id);

  if (hash_it != hash.end()) {
    return;
  }

  /*
  if (hash_it == hash.end()) {
    while (hash.size() >= capacity) {
      frame_id_t need_del = lst.back();
      lst.pop_back();
      if (hash.find(need_del) != hash.end()) {
        hash.erase(need_del);
      }
    }
  }
  */

  lst.push_back(frame_id);
  // std::prev(lst.end(),1) 返回lst.end()的前面的距离它为1的元素的迭代器
  // 即指向lst的最后一个实际元素的迭代器,也就是lst中指向末尾这个新插入的帧id的迭代器
  hash.emplace(frame_id, std::prev(lst.end(), 1));
}

size_t LRUReplacer::Size() {
  const std::lock_guard<mutex_t> guard(mutex);

  return hash.size();
}

}  // namespace bustub
