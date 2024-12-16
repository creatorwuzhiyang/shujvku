//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // NREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  auto bucket = dir_[index];
  return bucket->Find(key, value);

}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  //UNREACHABLE("not implemented");
   std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  auto bucket = dir_[index];
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  //UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  // bool ite=bucket->Insert(key,value)==false;
  // std::cout<<ite<<std::endl;
  size_t index = IndexOf(key);

  auto bucket=dir_[index];
  while(bucket->Insert(key,value)==false){
    if(global_depth_==bucket->GetDepth()){
      global_depth_++;
       DoubleDirectory();
    }
    SplitBucket(key);
    index = IndexOf(key);
    bucket = dir_[index];
    // bucket->(key,value);
  }

  
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::DoubleDirectory(){
  // std::cout<<"ddd"<<std::endl;
   size_t old_size = dir_.size();
    dir_.resize(2 * old_size);
  //  std::cout<<"ddd"<<std::endl;
    for (size_t i = 0; i < old_size; ++i) {
        dir_[i + old_size] = dir_[i];
    }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::SplitBucket(const K &key){
  size_t index = IndexOf(key);
    auto bucket=dir_[index];
    bucket->IncrementDepth();
    auto new_bucket=std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
    auto new_bucket2=std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());

      size_t step1=(1<<bucket->GetDepth())-1;
      size_t step=step1&index;
      // auto temp=dir_[index];
    for (size_t i = 0; i < dir_.size(); i++) {
        if((i&step1)!=step&&dir_[i]==bucket){
          dir_[i]=new_bucket;
        }
        else if((i&step1)==step&&dir_[i]==bucket){
          dir_[i]=new_bucket2;
        }
        }
    ++num_buckets_;
     auto items = bucket->GetItems();
     size_t tempp;
      for (auto item = items.begin(); item != items.end(); ++item) {
        tempp=std::hash<K>()(item->first);
          if((size_t)(step1&tempp)!=step){
            new_bucket->Insert(item->first,item->second);
          }
          else{
            new_bucket2->Insert(item->first,item->second);
          }   
    }
}
// template <typename K, typename V>
// void ExtendibleHashTable<K, V>::SplitBucket(const K &key){
//   size_t index = IndexOf(key);
//     auto bucket=dir_[index];
//     bucket->IncrementDepth();
//     auto new_bucket=std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
//     auto new_bucket2=std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());

//       size_t step=1<<(bucket->GetDepth()-1);
//       // size_t step=step1&index;
//       // auto temp=dir_[index];
//     for (size_t i = 0; i < dir_.size(); i++) {
//         if((i&step)!=0U&&dir_[i]==bucket){
//           dir_[i]=new_bucket;
//         }
//         else if((i&step)==0U&&dir_[i]==bucket){
//           dir_[i]=new_bucket2;
//         }
//         }
//     // dir_[index+step]=std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
//     ++num_buckets_;
//      auto items = bucket->GetItems();
//      size_t tempp;
//       for (auto item = items.begin(); item != items.end(); ++item) {
//         tempp=std::hash<K>()(item->first);
//           if((step&tempp)!=0U){
//             new_bucket->Insert(item->first,item->second);
//           }
//           else{
//             new_bucket2->Insert(item->first,item->second);
//           }
//     }
// }

template <typename K, typename V>
void ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket){

}
//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {

}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  //UNREACHABLE("not implemented");
    for (auto it = list_.begin(); it != list_.end(); ++it) {
        if(it->first==key){
          value=it->second;
            return true;
        }
    }
    return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  //UNREACHABLE("not implemented");
  for (auto it = list_.begin(); it != list_.end(); ++it) {
        if(it->first==key){
            list_.erase(it);
            return true;
        }
    }
    return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  //UNREACHABLE("not implemented");
  for (auto it = list_.begin(); it != list_.end(); ++it) {
        if(it->first==key){
            it->second=value;
            return true;
        }
    }
     if(IsFull()){
    return false;
  }
  list_.emplace_back(key,value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub




