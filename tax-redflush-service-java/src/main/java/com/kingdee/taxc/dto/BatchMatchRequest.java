package com.kingdee.taxc.dto;

import java.util.List;

public class BatchMatchRequest {
    private List<Long> billIds;

    public List<Long> getBillIds() { return billIds; }
    public void setBillIds(List<Long> billIds) { this.billIds = billIds; }
}
