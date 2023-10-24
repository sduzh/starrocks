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

#include "util/bthreads/semaphore.h"

#include <bthread/bthread.h>
#include <gtest/gtest.h>

#include <thread>

namespace starrocks::bthreads {

TEST(CountingSemaphoreTest, test_constructor) {
    CountingSemaphore semaphore1(2);
    CountingSemaphore semaphore2(0);
    EXPECT_TRUE(semaphore1.try_acquire());
    EXPECT_TRUE(semaphore1.try_acquire_for(std::chrono::microseconds(5)));
    EXPECT_FALSE(semaphore1.try_acquire());
    EXPECT_FALSE(semaphore2.try_acquire());
    EXPECT_FALSE(semaphore2.try_acquire_for(std::chrono::milliseconds(10)));
}

TEST(CountingSemaphoreTest, test_try_acquire_for) {
    CountingSemaphore sem(0);
    auto t0 = std::chrono::steady_clock::now();
    EXPECT_FALSE(sem.try_acquire_for(std::chrono::milliseconds(100)));
    auto t1 = std::chrono::steady_clock::now();
    auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    EXPECT_GE(cost, 100);
    EXPECT_LT(cost, 150);
}

TEST(CountingSemaphoreTest, test01) {
    constexpr int kMaxValue = 10;
    constexpr int kMaxThreads = 20;
    CountingSemaphore semaphore(kMaxValue);
    std::atomic<int> concurrency{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < kMaxThreads; i++) {
        threads.emplace_back([&, id=i]() {
            for (int loop = 0; loop < 100; loop++) {
                fprintf(stderr, "Thread %d waiting for semaphore at timestamp %ld\n", id, butil::gettimeofday_us());
                semaphore.acquire();
                fprintf(stderr, "Thread %d acquired semaphore at timestamp %ld\n", id, butil::gettimeofday_us());
                int value = concurrency.fetch_add(1, std::memory_order_relaxed);
                ASSERT_LE(value + 1, kMaxValue);
                concurrency.fetch_sub(1, std::memory_order_relaxed);
                semaphore.release();
            }
        });
    }

    for (auto&& t : threads) {
        t.join();
    }
    EXPECT_EQ(0, concurrency.load(std::memory_order_relaxed));
}

static constexpr int kMaxSempahoreValue = 10;
static std::atomic<int> g_max_bthreads{0};

static void* bthread_task(void* arg) {
    auto sem = (CountingSemaphore<INT_MAX>*)arg;
    for (int i = 0; i < 1000; i++) {
        sem->acquire();
        auto old = g_max_bthreads.fetch_add(1, std::memory_order_relaxed);
        CHECK_LE(old + 1, kMaxSempahoreValue);
        g_max_bthreads.fetch_sub(1, std::memory_order_relaxed);
        sem->release();
    }
    return nullptr;
}

TEST(CountingSemaphoreTest, test02) {
    constexpr int kMaxThreads = 1000;
    CountingSemaphore semaphore(kMaxSempahoreValue);
    std::vector<bthread_t> bthreads;
    std::vector<int> max_concurrency(kMaxThreads, 0);
    for (int i = 0; i < kMaxThreads; i++) {
        bthreads.emplace_back();
        ASSERT_EQ(0, bthread_start_background(&bthreads.back(), nullptr, bthread_task, &semaphore));
    }

    for (auto&& t : bthreads) {
        bthread_join(t, nullptr);
    }
    EXPECT_EQ(0, g_max_bthreads.load(std::memory_order_relaxed));
}

} // namespace starrocks::bthreads
