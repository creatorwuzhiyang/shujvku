//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {


LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);

    // 如果没有可驱逐的页面，返回false
    if (curr_size_ == 0) {
        return false;
    }

    // 优先从history_list中驱逐
    for (auto it = history_list_.begin(); it != history_list_.end(); ++it) {
        if (node_store_[*it].is_evictable) {
            *frame_id = *it;
            Remove(*frame_id);
            return true;
        }
    }

    // 如果history_list中没有可驱逐的页面，从cache_list中寻找
    frame_id_t victim_frame = -1;
    size_t max_distance = 0;

  for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it) {
        if (!node_store_[*it].is_evictable) {
            continue;
        }

        const auto &history = node_store_[*it].access_times;
        auto hist_it = history.begin();
        std::advance(hist_it, history.size() - k_);
        size_t distance = current_timestamp_ - *hist_it;

        if (victim_frame == -1 || distance > max_distance) {
            victim_frame = *it;
            max_distance = distance;
        }
    }

    if (victim_frame != -1) {
        *frame_id = victim_frame;
        Remove(victim_frame);
        return true;
    }

    return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);

    //BUSTUB_ASSERT(frame_id < replacer_size_, "Invalid frame_id!");
    BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame_id!");

    current_timestamp_++;

    // 处理新的frame
    if (node_store_.find(frame_id) == node_store_.end()) {
        node_store_[frame_id].access_times.push_back(current_timestamp_);
        history_list_.push_back(frame_id);
        return;
    }
  // 处理已存在的frame
    auto &history = node_store_[frame_id];
    history.access_times.push_back(current_timestamp_);

    // 判断是否达到k次访问
    if (history.access_times.size() == k_) {
        history_list_.remove(frame_id);
        cache_list_.push_back(frame_id);
    } else if (history.access_times.size() > k_) {
        // 维护访问历史大小
        if (history.access_times.size() > k_ + 1) {
            history.access_times.pop_front();
        }
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(latch_);

    //BUSTUB_ASSERT(frame_id < replacer_size_, "Invalid frame_id!");
    BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame_id!");

    auto it = node_store_.find(frame_id);
    if (it == node_store_.end()) {
        return;
    }

    auto &history = it->second;
    if (history.is_evictable == set_evictable) {
        return;
    }

    history.is_evictable = set_evictable;
    if (set_evictable) {
        curr_size_++;
    } else {
        curr_size_--;
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    // 调用Remove时已经加了锁，不需要重复加锁

    auto it = node_store_.find(frame_id);
    if (it == node_store_.end()) {
        return;
    }

    if (!it->second.is_evictable) {
        throw std::runtime_error("Cannot remove a non-evictable frame");
    }

    // 从对应的列表中移除
    if (it->second.access_times.size() < k_) {
        history_list_.remove(frame_id);
    } else {
        cache_list_.remove(frame_id);
    }

    // 从存储中移除
    node_store_.erase(frame_id);
    curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
    std::scoped_lock<std::mutex> lock(latch_);
    return curr_size_;
}

}  // namespace bustub


                              