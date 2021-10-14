//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"
#include <algorithm>

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages)
    : circular_{num_pages, ClockReplacer::Status::EMPTY_OR_PINNED}, hand_{0}, capacity_{num_pages} {
    //成员初始化列表语法
    circular_.reserve(num_pages);
}

ClockReplacer::~ClockReplacer() = default;

// try to find a victim frame in buffer pool with clock scheme
// and make the *frame_id = victim_frame_id
// not found, frame_id=nullptr and return false
bool ClockReplacer::Victim(frame_id_t* frame_id) {
    const std::lock_guard<mutex_t> guard(mutex_);
    size_t unEMPTY_OR_PINNED_count = 0;

    frame_id_t victim_frame_id = 0;

    // scan frame in the buffer pool
    for (size_t i = 0, idx = (hand_ + i) % capacity_; i < capacity_ + 1;
         i++, idx = (hand_ + i) % capacity_) {
        // The frame ref = '1' (ACCESSED) , some thread used it not so long ago
        if (circular_[idx] == ClockReplacer::Status::ACCESSED) {
            unEMPTY_OR_PINNED_count++;
            // make the frame ref = '0' , means this frame can be victim in the next scan
            circular_[idx] = ClockReplacer::Status::UNTOUCHED;
        } else if (circular_[idx] == ClockReplacer::Status::UNTOUCHED) {
            unEMPTY_OR_PINNED_count++;

            if (victim_frame_id == 0) {
                victim_frame_id = idx;
            }

            // victim_frame_id = victim_frame_id != 0 ? victim_frame_id : idx;
        }
    }

    // 0U or 0u means unsigned int 0
    // all frame condition EMPTY_OR_PINNED, have not storage page
    if (unEMPTY_OR_PINNED_count == 0U) {
        frame_id = nullptr;
        return false;
    }

    // all not EMPTY_OR_PINNED frame just changed from ACCESSED to UNTOUCHED in the above scan
    // scan again
    if (victim_frame_id == 0) {
        for (size_t i = 1, idx = (hand_ + i) % capacity_; i < capacity_ + 1;
             i++, idx = (hand_ + i) % capacity_) {
            if (circular_[idx] == ClockReplacer::Status::UNTOUCHED) {
                victim_frame_id = idx;
                break;
            }
        }
    }

    *frame_id = victim_frame_id;
    hand_ = victim_frame_id;  // update scan starter

    // circular_[victim_frame_id] = ClockReplacer::Status::EMPTY_OR_PINNED; // still works..
    // because this frame is victim , the page storage in the frame will write back to disk
    // now this frame can be seen as EMPTY_OR_PINNED
    circular_[victim_frame_id % capacity_] = ClockReplacer::Status::EMPTY_OR_PINNED;

    return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    const std::lock_guard<mutex_t> guard(mutex_);
    circular_[frame_id % capacity_] = ClockReplacer::Status::EMPTY_OR_PINNED;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    const std::lock_guard<mutex_t> guard(mutex_);
    circular_[frame_id % capacity_] = ClockReplacer::Status::ACCESSED;
}

size_t ClockReplacer::Size() {
    const std::lock_guard<mutex_t> guard(mutex_);

    //返回在[arg0, arg1)范围内满足特定条件(arg2)的元素的数目
    // return all items that in the range[circular_.begin, circular_.end )
    // and be met the condition: status!=EMPTY_OR_PINNED
    // That is the number of frames in the buffer pool that storage page (NOT EMPTY_OR_PINNED)
    return std::count_if(circular_.begin(), circular_.end(), [](ClockReplacer::Status status) {
        return status != ClockReplacer::Status::EMPTY_OR_PINNED;
    });
}

}  // namespace bustub
