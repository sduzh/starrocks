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

import com.starrocks.server.GlobalStateMgr;

class DropOlapPartitionAction extends DropOlapPartitionActionBase {
    DropOlapPartitionAction(long dbId, OlapTable table, Partition partition, boolean isForceDrop,
                            boolean reserveTablets) {
        super(dbId, table, partition, isForceDrop, reserveTablets);
    }

    @Override
    protected void afterDropPartition() {
        if (table.partitionInfo.isRangePartition()) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.partitionInfo;
            if (!isForceDrop) {
                // recycle range partition
                GlobalStateMgr.getCurrentRecycleBin().recyclePartition(dbId, table.id, partition,
                        rangePartitionInfo.getRange(partition.getId()),
                        rangePartitionInfo.getDataProperty(partition.getId()),
                        rangePartitionInfo.getReplicationNum(partition.getId()),
                        rangePartitionInfo.getIsInMemory(partition.getId()),
                        rangePartitionInfo.getDataCacheInfo(partition.getId()));
            } else if (!reserveTablets) {
                GlobalStateMgr.getCurrentState().onErasePartition(partition);
            }
        } else if (table.partitionInfo.getType() == PartitionType.LIST) {
            assert isForceDrop;
            if (!reserveTablets) {
                GlobalStateMgr.getCurrentState().onErasePartition(partition);
            }
        }
        GlobalStateMgr.getCurrentAnalyzeMgr().dropPartition(partition.getId());
    }
}
