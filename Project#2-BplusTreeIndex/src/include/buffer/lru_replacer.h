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
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the lru replacement policy, which approximates the Least Recently Used
 * policy.
 */
class LRUReplacer : public Replacer {
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

    bool Victim(frame_id_t* frame_id) override;

    void Pin(frame_id_t frame_id) override;

    void Unpin(frame_id_t frame_id) override;

    size_t Size() override;

   private:
    // TODO(student): implement me!
    using mutex_t = std::mutex;  //在LRUReplacer类中添加std::mutex,之后可用mutex_t代替
    mutex_t mutex;
    size_t capacity;
    std::list<frame_id_t> lst;  // lst队列中最前面的是最近访问最少的,最新访问的在最后
    std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> hash;
};

}  // namespace bustub
