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

#include "storage/rowset/series_column_iterator.h"

#include <gtest/gtest.h>

#include "column/fixed_length_column.h"
#include "testutil/assert.h"

namespace starrocks {

TEST(SeriesColumnIteratorTest, test01) {
    auto iter = SeriesColumnIterator<int>{0, 10};
    ASSERT_OK(iter.init(ColumnIteratorOptions{}));
    ASSERT_EQ(0, iter.get_current_ordinal());
    auto n = size_t{20};
    auto column = Int32Column{};
    ASSERT_OK(iter.next_batch(&n, &column));
    ASSERT_EQ(11, n);
    ASSERT_EQ(n, column.size());
    for (int i = 0; i < n; i++) {
        EXPECT_EQ(i, column.get(i).get_int32());
    }
    ASSERT_EQ(n, iter.get_current_ordinal());
    ASSERT_OK(iter.seek_to_first());
    column.reset_column();
    n = 3;
    ASSERT_OK(iter.next_batch(&n, &column));
    ASSERT_EQ(3, n);
    ASSERT_EQ(n, column.size());
    for (int i = 0; i < n; i++) {
        EXPECT_EQ(i, column.get(i).get_int32());
    }
    ASSERT_EQ(n, iter.get_current_ordinal());

    ASSERT_OK(iter.seek_to_ordinal(2));
    column.reset_column();
    n = 5;
    ASSERT_OK(iter.next_batch(&n, &column));
    ASSERT_EQ(5, n);
    ASSERT_EQ(n, column.size());
    for (int i = 0; i < n; i++) {
        EXPECT_EQ(i + 2, column.get(i).get_int32());
    }
    ASSERT_EQ(n + 2, iter.get_current_ordinal());

    column.reset_column();
    n = 5;
    ASSERT_OK(iter.next_batch(&n, &column));
    ASSERT_EQ(4, n);
    ASSERT_EQ(n, column.size());
    for (int i = 0; i < n; i++) {
        EXPECT_EQ(7 + i, column.get(i).get_int32());
    }
    ASSERT_EQ(11, iter.get_current_ordinal());
}

TEST(SeriesColumnIteratorTest, test02) {
    auto iter = SeriesColumnIterator<int>{0, 10, 2};
    auto n = size_t{20};
    auto column = Int32Column{};
    ASSERT_OK(iter.next_batch(&n, &column));
    ASSERT_EQ(6, n);
    ASSERT_EQ(n, column.size());
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(i * 2, column.get(i).get_int32());
    }
    ASSERT_EQ(6, iter.get_current_ordinal());

    column.reset_column();
    ASSERT_OK(iter.seek_to_ordinal(2));
    n = 2;
    ASSERT_OK(iter.next_batch(&n, &column));
    ASSERT_EQ(2, n);
    ASSERT_EQ(n, column.size());
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(4 + i * 2, column.get(i).get_int32());
    }
    ASSERT_EQ(4, iter.get_current_ordinal());
}

TEST(SeriesColumnIteratorTest, test03) {
    auto iter = SeriesColumnIterator<int>{0, -9, -1};
    ASSERT_OK(iter.init(ColumnIteratorOptions{}));
    auto n = size_t{4};
    auto column = Int32Column{};
    ASSERT_OK(iter.next_batch(&n, &column));
    ASSERT_EQ(4, n);
    ASSERT_EQ(n, column.size());
    EXPECT_EQ(n, iter.get_current_ordinal());
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(-i, column.get(i).get_int32());
    }
}

TEST(SeriesColumnIteratorTest, test04) {
    auto iter = SeriesColumnIterator<int>{0, -9, -3};
    ASSERT_OK(iter.init(ColumnIteratorOptions{}));
    auto n = size_t{4};
    auto column = Int32Column{};
    ASSERT_OK(iter.next_batch(&n, &column));
    ASSERT_EQ(4, n);
    ASSERT_EQ(n, column.size());
    EXPECT_EQ(n, iter.get_current_ordinal());
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(i * -3, column.get(i).get_int32());
    }
}

TEST(SeriesColumnIteratorTest, test05) {
    auto iter = SeriesColumnIterator<int>{0, -9, 0};
    ASSERT_FALSE(iter.init(ColumnIteratorOptions{}).ok());

    auto iter2 = SeriesColumnIterator<int>{0, 9, 0};
    ASSERT_FALSE(iter2.init(ColumnIteratorOptions{}).ok());

    auto iter3 = SeriesColumnIterator<int>{0, 9, -1};
    ASSERT_FALSE(iter3.init(ColumnIteratorOptions{}).ok());

    auto iter4 = SeriesColumnIterator<int>{20, 9, 1};
    ASSERT_FALSE(iter4.init(ColumnIteratorOptions{}).ok());

    auto iter5 = SeriesColumnIterator<int>{1, 2, 2};
    ASSERT_OK(iter5.init(ColumnIteratorOptions{}));
    auto n = size_t{2};
    auto column = Int32Column{};
    ASSERT_OK(iter5.next_batch(&n, &column));
    ASSERT_EQ(1, n);
    ASSERT_EQ(n, column.size());
    ASSERT_EQ(1, column.get(0).get_int32());
    ASSERT_EQ(1, iter5.get_current_ordinal());

    auto iter6 = SeriesColumnIterator<int>{-1, -2, -2};
    column.reset_column();
    n = 2;
    ASSERT_OK(iter6.next_batch(&n, &column));
    ASSERT_EQ(1, n);
    ASSERT_EQ(n, column.size());
    ASSERT_EQ(-1, column.get(0).get_int32());
    ASSERT_EQ(1, iter6.get_current_ordinal());
}

// test overflow
TEST(SeriesColumnIteratorTest, test06) {
    auto iter = SeriesColumnIterator<uint8_t>{250, 255, 2};
    ASSERT_OK(iter.init(ColumnIteratorOptions{}));
    auto n = size_t{10};
    auto column = Int8Column{};
    ASSERT_OK(iter.next_batch(&n, &column));
    ASSERT_EQ(3, n);
    ASSERT_EQ(n, column.size());
    EXPECT_EQ(n, iter.get_current_ordinal());
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(250 + i * 2, column.get(i).get_uint8());
    }
    ASSERT_OK(iter.next_batch(&n, &column));
    ASSERT_EQ(0, n);
}

// test underflow
TEST(SeriesColumnIteratorTest, test07) {
    auto iter = SeriesColumnIterator<int8_t>{-124, -128, -2};
    ASSERT_OK(iter.init(ColumnIteratorOptions{}));
    auto n = size_t{10};
    auto column = Int8Column{};
    ASSERT_OK(iter.next_batch(&n, &column));
    ASSERT_EQ(3, n);
    ASSERT_EQ(n, column.size());
    EXPECT_EQ(n, iter.get_current_ordinal());
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(-124 - i * 2, column.get(i).get_int8());
    }
    ASSERT_OK(iter.next_batch(&n, &column));
    ASSERT_EQ(0, n);
}
} // namespace starrocks