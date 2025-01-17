// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import org.junit.Assert;

public class MemoStatusChecker {
    private final int relNum;
    private final Memo memo;
    private final ColumnRefSet outputColumns;

    MemoStatusChecker(Memo memo, int relNum, ColumnRefSet outputColumns) {
        this.memo = memo;
        this.relNum = relNum;
        this.outputColumns = outputColumns;
    }

    public void checkStatus() {
        long groupNum = getGroupsNum();
        Assert.assertEquals(groupNum, memo.getGroups().size());

        long logicalMExprNum = getLogicalMExprNum();
        Assert.assertEquals(logicalMExprNum, getLogicalGroupExprsSize());

        long physicalMExprNum = getPhysicalMExprNum();
        Assert.assertEquals(physicalMExprNum, getPhysicalGroupExprsSize());

        long planNum = getPlanNum();
        Assert.assertEquals(planNum, getPlanCount(memo.getRootGroup()));

        LogicalProperty logicalProperty = memo.getRootGroup().getLogicalProperty();
        Assert.assertTrue(logicalProperty.getOutputColumns().isSame(outputColumns));
    }

    private int getLogicalGroupExprsSize() {
        int count = 0;
        for (GroupExpression expression : memo.getGroupExpressions().keySet()) {
            if (expression.getOp().isLogical()) {
                count++;
            }
        }
        return count;
    }

    private int getPhysicalGroupExprsSize() {
        int count = 0;
        for (GroupExpression expression : memo.getGroupExpressions().keySet()) {
            if (expression.getOp().isPhysical()) {
                count++;
            }
        }
        return count;
    }

    private long getGroupsNum() {
        return (long) (Math.pow(2, relNum) + 0.5) - 1;
    }

    private long getLogicalMExprNum() {
        return (long) (Math.pow(3, relNum) - Math.pow(2, relNum + 1) + 1) + relNum;
    }

    private long getPhysicalMExprNum() {
        return getLogicalMExprNum();
    }

    private long getPlanNum() {
        return factorial(2 * relNum - 2) / factorial(relNum - 1);
    }

    private long getPlanCount(Group root) {
        long planCountTotal = 0;
        for (GroupExpression mExpr : root.getPhysicalExpressions()) {
            long planCount = 1;
            for (Group child : mExpr.getInputs()) {
                planCount *= getPlanCount(child);
            }
            planCountTotal += planCount;
        }
        return planCountTotal;
    }

    private long factorial(long num) {
        long sum = 1;
        if (num < 0) {
            return num;
        }
        if (num == 1) {
            return 1;
        } else {
            sum = num * factorial(num - 1);
            return sum;
        }
    }
}
