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

#include "storage/rowset/column_type_converter.h"

#include <gtest/gtest.h>

#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/series_column_iterator.h"
#include "testutil/assert.h"
#include "util/defer_op.h"

namespace starrocks {

class ColumnTypeConverterTestBase : public ::testing::Test {};

class ColumnTypeConverterWithDefaultValueColumnIteratorTest : public ColumnTypeConverterTestBase {};

TEST_F(ColumnTypeConverterWithDefaultValueColumnIteratorTest, test01) {
    auto source_type = LogicalType::TYPE_INT;
    auto target_type = LogicalType::TYPE_BIGINT;
    auto source_iter = new DefaultValueColumnIterator(true, "NULL", true, get_type_info(source_type), 0, 2);
    auto converter = new ColumnTypeConverter(source_iter, source_type, target_type, true);
    DeferOp defer([&]() { delete converter; });
    auto opts = ColumnIteratorOptions{};
    ASSERT_OK(converter->init(opts));
    auto column = NullableColumn(Int64Column::create(), NullColumn ::create());
    auto range = SparseRange<>{0, 2};
    ASSERT_OK(converter->next_batch(range, &column));
    ASSERT_EQ(2, column.size());
    ASSERT_TRUE(column.is_null(0));
    ASSERT_TRUE(column.is_null(1));
}

TEST_F(ColumnTypeConverterWithDefaultValueColumnIteratorTest, test02) {
    auto source_type = LogicalType::TYPE_INT;
    auto target_type = LogicalType::TYPE_BIGINT;
    auto source_iter = new DefaultValueColumnIterator(true, "10", false, get_type_info(source_type), 0, 2);
    auto converter = new ColumnTypeConverter(source_iter, source_type, target_type, true);
    DeferOp defer([&]() { delete converter; });
    auto opts = ColumnIteratorOptions{};
    ASSERT_OK(converter->init(opts));
    auto column = NullableColumn(Int64Column::create(), NullColumn ::create());
    auto range = SparseRange<>{0, 2};
    ASSERT_OK(converter->next_batch(range, &column));
    ASSERT_EQ(2, column.size());
    ASSERT_FALSE(column.is_null(0));
    ASSERT_FALSE(column.is_null(1));
    ASSERT_EQ(10, column.get(0).get_int64());
    ASSERT_EQ(10, column.get(1).get_int64());
}

namespace {
struct TestParameter {
    bool nullable_source;
    bool nullable_target;
};
} // namespace

class ColumnTypeConverterWithNumericTypeTest : public ColumnTypeConverterTestBase,
                                               public ::testing::WithParamInterface<TestParameter> {};

TEST_P(ColumnTypeConverterWithNumericTypeTest, test) {
    auto source_type = LogicalType::TYPE_SMALLINT;
    auto target_type = LogicalType::TYPE_BIGINT;
    auto source_iter = new SeriesColumnIterator<int16_t>(0, 31);
    auto target_iter = new ColumnTypeConverter(source_iter, source_type, target_type, GetParam().nullable_source);
    DeferOp defer([&]() { delete target_iter; });
    auto opts = ColumnIteratorOptions{};
    ASSERT_OK(target_iter->init(opts));
    auto column = (GetParam().nullable_target)
                          ? (ColumnPtr)NullableColumn::create(Int64Column::create(), NullColumn::create())
                          : (ColumnPtr)Int64Column::create();
    auto n = size_t{10};
    ASSERT_OK(target_iter->next_batch(&n, column.get()));
    ASSERT_EQ(10, n);
    ASSERT_EQ(n, column->size());
    ASSERT_EQ(n, target_iter->get_current_ordinal());
    EXPECT_FALSE(column->has_null());
    for (int i = 0; i < n; i++) {
        EXPECT_FALSE(column->is_null(i));
        EXPECT_EQ(i, column->get(i).get_int64());
    }

    n = 30;
    ASSERT_OK(target_iter->next_batch(&n, column.get()));
    ASSERT_EQ(22, n);
    ASSERT_EQ(32, column->size());
    ASSERT_EQ(32, target_iter->get_current_ordinal());
    for (int i = 0; i < 32; i++) {
        EXPECT_FALSE(column->is_null(i));
        EXPECT_EQ(i, column->get(i).get_int64());
    }
    ASSERT_OK(target_iter->next_batch(&n, column.get()));
    ASSERT_EQ(0, n);
    ASSERT_EQ(32, column->size());
}
INSTANTIATE_TEST_SUITE_P(numeric_test, ColumnTypeConverterWithNumericTypeTest,
                         testing::Values(TestParameter{false, false}, TestParameter{false, true},
                                         TestParameter{true, false}, TestParameter{true, true}));

} // namespace starrocks