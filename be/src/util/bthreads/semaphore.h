// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <bthread/butex.h>

#include <chrono>
#include <iostream>

#include "bthread/processor.h" // cpu_relax()
#include "util/time.h"

namespace starrocks::bthreads {

template <int least_max_value = INT_MAX>
class CountingSemaphore {
    static_assert(least_max_value >= 0);
    static_assert(least_max_value <= INT_MAX);

public:
    explicit CountingSemaphore(int desired) {
        _counter = bthread::butex_create_checked<butil::atomic<int>>();
        _counter->store(desired, butil::memory_order_release);
    }

    ~CountingSemaphore() { bthread::butex_destroy(_counter); }

    CountingSemaphore(const CountingSemaphore&) = delete;
    CountingSemaphore& operator=(const CountingSemaphore&) = delete;

    static constexpr int max() noexcept { return least_max_value; }

    void release(int update = 1) {
        if (_counter->fetch_add(update, butil::memory_order_release) > 0) {
            return;
        }
        // else
        if (update == 1) {
            (void)bthread::butex_wake(_counter);
        } else {
            (void)bthread::butex_wake_all(_counter);
        }
    }

    void acquire() {
        CHECK(do_try_acquire_until(nullptr));
    }

    bool try_acquire() {
        constexpr int kMaxRetry = 12;
        for (int i = 0; i < kMaxRetry; i++) {
            if (do_try_acquire()) {
                return true;
            }
            cpu_relax();
        }
        return false;
    }

    template <typename Rep, typename Period>
    bool try_acquire_for(const std::chrono::duration<Rep, Period>& dur) {
        return try_acquire_until(std::chrono::system_clock::now() +
                                 std::chrono::ceil<std::chrono::system_clock::duration>(dur));
    }

    template <typename Clock, typename Duration>
    bool try_acquire_until(const std::chrono::time_point<Clock, Duration>& abs) {
        timespec ts = TimespecFromTimePoint(abs);
        return do_try_acquire_until(&ts);
    }

private:
    bool do_try_acquire() {
        auto old = _counter->load(butil::memory_order_acquire);
        if (old == 0) return false;
        return _counter->compare_exchange_strong(old, old - 1, butil::memory_order_acquire,
                                                 butil::memory_order_relaxed);
    }

    bool do_try_acquire_until(const timespec* ts) {
        while (true) {
            auto old = _counter->load(butil::memory_order_acquire);
            if (old == 0) {
                if (bthread::butex_wait(_counter, old, ts) < 0) {
                    if (errno == ETIMEDOUT) {
                        return false;
                    } else if (errno != EWOULDBLOCK && errno != EINTR) {
                        PLOG(WARNING) << "Fail to wait butex";
                    }
                }
                continue;
            }
            DCHECK(old > 0);
            if (_counter->compare_exchange_strong(old, old - 1, butil::memory_order_acquire,
                                                  butil::memory_order_relaxed)) {
                return true;
            }
        }
    }

    butil::atomic<int>* _counter;
};

using BinarySemaphore = CountingSemaphore<1>;

} // namespace starrocks::bthreads
