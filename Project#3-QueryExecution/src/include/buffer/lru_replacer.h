//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the lru replacement policy, which approximates the Least Recently Used
 * policy.
 */
class LRUReplacer : public Replacer {
  using mutex_t = std::mutex;

 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // TODO(student): implement me!
  // concurrent mutex
  mutex_t mutex;
  //
  size_t capacity;
  //
  std::list<frame_id_t> lru_list;
  // LRU可淘汰帧id为键,lru_list中指向对应frame_id_t的迭代器为值的unordered_map
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> lru_map;
};

}  // namespace bustub
