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

package com.starrocks.catalog;

import com.starrocks.common.io.Writable;

/**
 * RecycleBinPartitionCleaner is responsible for completely cleaning up and removing partitions
 * that have been dropped and moved to the recycle bin.
 * <p>
 * When a partition is dropped via ALTER TABLE DROP PARTITION, it is moved to the recycle bin
 * and can still be recovered with RECOVER PARTITION xxx. This class handles the next step of permanently
 * deleting dropped partitions from the recycle bin.
 * <p>
 * After RecycleBinPartitionCleaner deletes a partition, it cannot be recovered with RECOVER PARTITION xxx.
 */
public abstract class RecycleBinPartitionCleaner implements Writable {
    abstract boolean supportRetry();

    abstract void cleanPartition(Partition partition);

    abstract boolean needRetry();
}
