//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <algorithm>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the
  // replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to
  // P.
  const std::lock_guard<std::mutex> guard(latch_);

  // try to find the page_id page in the bufferPool
  // use bufferPool map <page, frame> , the buffer contains frames to storage page
  std::unordered_map<page_id_t, frame_id_t>::iterator table_item_it = page_table_.find(page_id);

  frame_id_t frame_id;

  // find the id page in bufferpool successfully
  if (table_item_it != page_table_.end()) {
    frame_id = table_item_it->second;
    Page *the_page_in_frame = &pages_[frame_id];
    // the page in frame 's pin_count need +1
    // to point out another thread using the page which is storaged in buffer frame
    ++the_page_in_frame->pin_count_;
    // Pin this frame, can not be victim frame
    replacer_->Pin(frame_id);

    return the_page_in_frame;
  }

  if (!free_list_.empty()) {
    // use the free list the first front frame as frame will be used to storage the page
    // move the needed page in the frame
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    bool victim_found = replacer_->Victim(&frame_id);
    // try to find victim frame to storage the needed page
    // the frame_id will be update to the victim frame id
    if (!victim_found) {
      return nullptr;
    }
  }
  // find victim successfully, frame_id update to the victim frame id
  Page *replacedPage_in_frame = &pages_[frame_id];

  if (replacedPage_in_frame->IsDirty()) {
    disk_manager_->WritePage(replacedPage_in_frame->GetPageId(), replacedPage_in_frame->GetData());
    replacedPage_in_frame->is_dirty_ = false;
  }

  page_table_.erase(replacedPage_in_frame->GetPageId());

  // create new element <page_id_t, frame_id_t> in page_table
  page_table_.emplace(page_id, frame_id);

  replacedPage_in_frame->page_id_ = page_id;

  ++replacedPage_in_frame->pin_count_;

  replacer_->Pin(frame_id);

  disk_manager_->ReadPage(page_id, replacedPage_in_frame->GetData());

  // now the needed page is storaged in the frame_id frame's replaced page's position
  return replacedPage_in_frame;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  const std::lock_guard<std::mutex> guard(latch_);

  std::unordered_map<page_id_t, frame_id_t>::iterator table_item_it = page_table_.find(page_id);

  // can not find the page in the bufferPool frame
  // this page is already unpined
  if (table_item_it == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = table_item_it->second;

  Page *thePage_in_frame = &pages_[frame_id];

  int pin_count = thePage_in_frame->GetPinCount();

  if (is_dirty) {
    thePage_in_frame->is_dirty_ = true;
  }

  if (pin_count <= 0) {
    return false;
  }

  thePage_in_frame->pin_count_--;

  if (thePage_in_frame->GetPinCount() == 0) {
    replacer_->Unpin(frame_id);
  }

  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!

  // make sure the page_id is valid
  assert(page_id != INVALID_PAGE_ID);

  const std::lock_guard<std::mutex> guard(latch_);

  std::unordered_map<page_id_t, frame_id_t>::iterator table_item_it = page_table_.find(page_id);

  if (table_item_it == page_table_.end()) {
    return false;
  }

  Page *thePage_in_frame = &pages_[table_item_it->second];

  disk_manager_->WritePage(thePage_in_frame->GetPageId(), thePage_in_frame->GetData());

  thePage_in_frame->is_dirty_ = false;

  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the
  // free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  const std::lock_guard<std::mutex> guard(latch_);

  bool all_pinned = true;

  for (size_t i = 0; i < pool_size_; i++) {
    // find a not pinned page
    if (pages_[i].GetPinCount() <= 0) {
      all_pinned = false;
      break;
    }
  }

  if (all_pinned) {
    return nullptr;
  }

  frame_id_t frame_id;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    bool victim_found = replacer_->Victim(&frame_id);
    if (!victim_found) {
      return nullptr;
    }
  }

  Page *replacedPage_in_frame = &pages_[frame_id];

  if (replacedPage_in_frame->IsDirty()) {
    disk_manager_->WritePage(replacedPage_in_frame->GetPageId(), replacedPage_in_frame->GetData());
    replacedPage_in_frame->is_dirty_ = false;
  }

  page_table_.erase(replacedPage_in_frame->GetPageId());

  page_id_t new_page_id = disk_manager_->AllocatePage();

  replacedPage_in_frame->page_id_ = new_page_id;

  replacedPage_in_frame->pin_count_++;

  replacedPage_in_frame->ResetMemory();

  replacer_->Pin(frame_id);

  page_table_.emplace(new_page_id, frame_id);

  *page_id = new_page_id;

  return replacedPage_in_frame;
}

// delete a page from bufferPool
bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return
  // it to the free list.
  const std::lock_guard<std::mutex> guard(latch_);

  std::unordered_map<page_id_t, frame_id_t>::iterator table_item_it = page_table_.find(page_id);

  if (table_item_it == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = table_item_it->second;

  Page *replacedPage_in_frame = &pages_[frame_id];

  // the page still used by some thread, can not deleted(replaced)
  if (replacedPage_in_frame->GetPinCount() != 0) {
    return false;
  }

  if (replacedPage_in_frame->IsDirty()) {
    disk_manager_->WritePage(replacedPage_in_frame->GetPageId(), replacedPage_in_frame->GetData());
    replacedPage_in_frame->is_dirty_ = false;
  }

  page_table_.erase(replacedPage_in_frame->GetPageId());

  disk_manager_->DeallocatePage(page_id);

  replacedPage_in_frame->ResetMemory();
  replacedPage_in_frame->page_id_ = INVALID_PAGE_ID;
  replacedPage_in_frame->pin_count_ = 0;
  replacedPage_in_frame->is_dirty_ = false;

  free_list_.push_back(frame_id);

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  const std::lock_guard<std::mutex> guard(latch_);

  for (size_t i = 0; i < pool_size_; i++) {
    Page *thePage_in_frame = &pages_[i];
    if (thePage_in_frame->page_id_ != INVALID_PAGE_ID && thePage_in_frame->IsDirty()) {
      disk_manager_->WritePage(thePage_in_frame->GetPageId(), thePage_in_frame->GetData());
      thePage_in_frame->is_dirty_ = false;
    }
  }
}

}  // namespace bustub
