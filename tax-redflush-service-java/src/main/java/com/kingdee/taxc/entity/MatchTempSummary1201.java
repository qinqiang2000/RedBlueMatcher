package com.kingdee.taxc.entity;

import java.math.BigDecimal;

public class MatchTempSummary1201 {
    private Long jobid;
    private String fspbm;
    private Long itemCount;
    private BigDecimal totalAmount;

    public Long getJobid() { return jobid; }
    public void setJobid(Long jobid) { this.jobid = jobid; }
    public String getFspbm() { return fspbm; }
    public void setFspbm(String fspbm) { this.fspbm = fspbm; }
    public Long getItemCount() { return itemCount; }
    public void setItemCount(Long itemCount) { this.itemCount = itemCount; }
    public BigDecimal getTotalAmount() { return totalAmount; }
    public void setTotalAmount(BigDecimal totalAmount) { this.totalAmount = totalAmount; }
}
