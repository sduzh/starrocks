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

#include <type_traits>

#include "storage/rowset/column_iterator.h"

namespace starrocks {

// SeriesColumnIterator is used to generate a series of numbers, used in tests.
template <typename T>
class SeriesColumnIterator : public ColumnIterator {
    static_assert(std::is_integral<T>::value);

public:
    explicit SeriesColumnIterator(T start, T stop) : SeriesColumnIterator(start, stop, 1) {}
    explicit SeriesColumnIterator(T start, T stop, T step) : _start(start), _stop(stop), _step(step), _offset(0) {}

    ~SeriesColumnIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override {
        if (_step == 0) {
            return Status::InvalidArgument("step cannot be zero");
        }
        if (_step > 0 && _stop < _start) {
            return Status::InvalidArgument("step is positive, stop should be greater than start");
        }
        if (_step < 0 && _stop > _start) {
            return Status::InvalidArgument("step is negative, stop should be less than start");
        }
        return Status::OK();
    }

    Status seek_to_first() override {
        _offset = 0;
        return Status::OK();
    }

    Status seek_to_ordinal(ordinal_t ord) override {
        _offset = ord;
        return Status::OK();
    }

    Status next_batch(size_t* n, Column* dst) override;

    Status next_batch(const SparseRange<>& range, Column* dst) override;

    ordinal_t get_current_ordinal() const override { return _offset; }

    ordinal_t num_rows() const override { return 1 + (_stop - _start) / _step; }

private:
    T _start;
    T _stop;
    T _step;
    ordinal_t _offset;
};

template <typename T>
inline Status SeriesColumnIterator<T>::next_batch(size_t* n, starrocks::Column* dst) {
    auto num_rows = ordinal_t{1} + (_stop - _start) / _step;
    *n = std::min<size_t>(*n, num_rows - _offset);
    for (size_t i = 0; i < *n; i++) {
        auto curr = _start + _offset * _step;
        [[maybe_unused]] bool r = dst->append_numbers(&curr, sizeof(T));
        DCHECK(r);
        _offset++;
    }
    return Status::OK();
}

template <typename T>
inline Status SeriesColumnIterator<T>::next_batch(const SparseRange<>& range, Column* dst) {
    return Status::NotSupported("SeriesColumnIterator::next_batch not supported");
}

} // namespace starrocks