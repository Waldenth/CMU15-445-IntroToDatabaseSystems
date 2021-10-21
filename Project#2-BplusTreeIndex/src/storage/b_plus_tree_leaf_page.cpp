//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
// B_PLUS_TREE_LEAF_PAGE_TYPE -->
// BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
// template macro
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetSize(0);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetNextPageId(INVALID_PAGE_ID);
    SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
    return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
    next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType& key,
                                         const KeyComparator& comparator) const {
    // k_it is the first MappingType whose key >= arg: const KeyType& key
    // lower_bound need comp return true if arg1<arg2
    // const MappingType& pair, KeyType k
    auto k_it = std::lower_bound(
        array, array + GetSize(), key,
        [&comparator](const auto& pair, auto k) { return comparator(pair.first, k) < 0; });

    return std::distance(array, k_it);
    // return 0;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
    // replace with your own code
    return array[index].first;
    // KeyType key{};
    // return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType& B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
    // replace with your own code
    return array[index];
    // return array[0];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType& key,
                                       const ValueType& value,
                                       const KeyComparator& comparator) {
    /*
    auto k_it = std::lower_bound(
        array, array + GetSize(), key,
        [&comparator](const auto& pair, auto k) { return comparator(pair.first, k) < 0; });
    */
    // Get the first MappingType k_it whose key >= arg: const KeyType& key
    auto k_it = std::lower_bound(
        array, array + GetSize(), key,
        [&comparator](const auto& pair, auto k) { return comparator(pair.first, k) < 0; });
    if (k_it == array + GetSize()) {
        k_it->first = key;
        k_it->second = value;
        IncreaseSize(1);
        return GetSize();
    }

    // is a question
    /*
    if (comparator(k_it->first, key) == 0) {
        return GetSize();
    }
    */

    std::move_backward(k_it, array + GetSize(), array + GetSize() + 1);

    k_it->first = key;
    k_it->second = value;
    IncreaseSize(1);

    return GetSize();

    // return 0;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage* recipient) {
    int start_index = GetMinSize();
    int moved_size = GetMaxSize() - start_index;
    recipient->CopyNFrom(array + start_index, moved_size);
    IncreaseSize(moved_size * -1);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType* items, int size) {
    // copy elements [items,items+size) to the place start at array+GetSize()
    std::copy(items, items + size, array + GetSize());
    IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType& key,
                                        ValueType* value,
                                        const KeyComparator& comparator) const {
    auto k_it = std::lower_bound(
        array, array + GetSize(), key,
        [&comparator](const auto& pair, auto k) { return comparator(pair.first, k) < 0; });

    if (k_it == array + GetSize() || comparator(k_it->first, key) != 0) {
        return false;
    }

    *value = k_it->second;
    return true;

    // return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType& key,
                                                      const KeyComparator& comparator) {
    auto k_it = std::lower_bound(
        array, array + GetSize(), key,
        [&comparator](const auto& pair, auto k) { return comparator(pair.first, k) < 0; });
    // not find
    if (k_it == array + GetSize() || comparator(k_it->first, key) != 0) {
        return GetSize();
    }

    // move elements [k_it+1,array+GetSize()) to the place start at k_it
    std::move(k_it + 1, array + GetSize(), k_it);
    IncreaseSize(-1);
    return GetSize();

    // return 0;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage* recipient) {
    recipient->CopyNFrom(array, GetSize());
    recipient->SetNextPageId(GetNextPageId());
    IncreaseSize(-1 * GetSize());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage* recipient) {
    auto first_item = GetItem(0);
    std::move(array + 1, array + GetSize(), array);
    IncreaseSize(-1);

    recipient->CopyLastFrom(first_item);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType& item) {
    *(array + GetSize()) = item;
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage* recipient) {
    auto last_item = GetItem(GetSize() - 1);
    IncreaseSize(-1);

    recipient->CopyFirstFrom(last_item);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType& item) {
    std::move_backward(array, array + GetSize(), array + GetSize() + 1);
    *array = item;
    IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
