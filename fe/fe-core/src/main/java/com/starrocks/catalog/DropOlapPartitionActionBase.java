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

import com.starrocks.sql.analyzer.SemanticException;

import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class DropOlapPartitionActionBase implements DropPartitionAction {
    protected final long dbId;
    protected final OlapTable table;
    protected final Partition partition;
    protected final boolean isForceDrop;
    protected final boolean reserveTablets;

    public DropOlapPartitionActionBase(long dbId, OlapTable table, Partition partition, boolean isForceDrop,
                                boolean reserveTablets) {
        this.dbId = dbId;
        this.table = requireNonNull(table, "table is null");
        this.partition = requireNonNull(partition, "partition is null");
        this.isForceDrop = isForceDrop;
        this.reserveTablets = reserveTablets;
    }

    @Override
    public void execute() {
        beforeDropPartition();
        dropPartition();
        afterDropPartition();
    }

    protected void beforeDropPartition() {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        if (partitionInfo.isListPartition() && !isForceDrop) {
            throw new SemanticException(String.format("Cannot drop list partition using this command. " +
                    "Use \"ALTER TABLE `%s` DROP PARTITION `%s` FORCE\" to permanently drop list partition." +
                    "Dropped list partition cannot be recovered.", table.getName(), partition.getName()));
        }
    }

    protected void dropPartition() {
        table.idToPartition.remove(partition.getId());
        table.nameToPartition.remove(partition.getName());
        partition.getSubPartitions()
                .stream().map(PhysicalPartition::getId)
                .collect(Collectors.toList()).forEach(table.physicalPartitionIdToPartitionId.keySet()::remove);
        table.getPartitionInfo().dropPartition(partition.getId());
    }

    protected void afterDropPartition() {
    }
}
