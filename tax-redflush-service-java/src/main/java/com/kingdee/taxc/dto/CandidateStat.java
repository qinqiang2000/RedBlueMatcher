package com.kingdee.taxc.dto;

import java.math.BigDecimal;

public class CandidateStat {
    private Long cnt;
    private BigDecimal sumAmount;

    public Long getCnt() { return cnt; }
    public void setCnt(Long cnt) { this.cnt = cnt; }
    public BigDecimal getSumAmount() { return sumAmount; }
    public void setSumAmount(BigDecimal sumAmount) { this.sumAmount = sumAmount; }
}
