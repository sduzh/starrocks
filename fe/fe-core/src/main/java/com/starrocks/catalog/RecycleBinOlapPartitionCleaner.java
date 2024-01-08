package com.starrocks.catalog;

import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;

import java.io.DataOutput;
import java.io.IOException;

public class RecycleBinOlapPartitionCleaner extends RecycleBinPartitionCleaner {
    @Override
    boolean supportRetry() {
        return false;
    }

    @Override
    void cleanPartition(Partition partition) {
        GlobalStateMgr.getCurrentState().onErasePartition(partition);
    }

    @Override
    boolean needRetry() {
        return false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}
