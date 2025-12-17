package com.kingdee.taxc.entity;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class MatchResult1201 {
    private Long fid;
    private Long fbillid;
    private String fbuyertaxno;
    private String fsalertaxno;
    private String fspbm;
    private Long finvoiceid;
    private Long finvoiceitemid;
    private BigDecimal fnum;
    private BigDecimal fbillamount;
    private BigDecimal finvoiceamount;
    private BigDecimal fmatchamount;
    private BigDecimal fbillunitprice;
    private BigDecimal fbillqty;
    private BigDecimal finvoiceunitprice;
    private BigDecimal finvoiceqty;
    private LocalDateTime fmatchtime;

    public Long getFid() { return fid; }
    public void setFid(Long fid) { this.fid = fid; }
    public Long getFbillid() { return fbillid; }
    public void setFbillid(Long fbillid) { this.fbillid = fbillid; }
    public String getFbuyertaxno() { return fbuyertaxno; }
    public void setFbuyertaxno(String fbuyertaxno) { this.fbuyertaxno = fbuyertaxno; }
    public String getFsalertaxno() { return fsalertaxno; }
    public void setFsalertaxno(String fsalertaxno) { this.fsalertaxno = fsalertaxno; }
    public String getFspbm() { return fspbm; }
    public void setFspbm(String fspbm) { this.fspbm = fspbm; }
    public Long getFinvoiceid() { return finvoiceid; }
    public void setFinvoiceid(Long finvoiceid) { this.finvoiceid = finvoiceid; }
    public Long getFinvoiceitemid() { return finvoiceitemid; }
    public void setFinvoiceitemid(Long finvoiceitemid) { this.finvoiceitemid = finvoiceitemid; }
    public BigDecimal getFnum() { return fnum; }
    public void setFnum(BigDecimal fnum) { this.fnum = fnum; }
    public BigDecimal getFbillamount() { return fbillamount; }
    public void setFbillamount(BigDecimal fbillamount) { this.fbillamount = fbillamount; }
    public BigDecimal getFinvoiceamount() { return finvoiceamount; }
    public void setFinvoiceamount(BigDecimal finvoiceamount) { this.finvoiceamount = finvoiceamount; }
    public BigDecimal getFmatchamount() { return fmatchamount; }
    public void setFmatchamount(BigDecimal fmatchamount) { this.fmatchamount = fmatchamount; }
    public BigDecimal getFbillunitprice() { return fbillunitprice; }
    public void setFbillunitprice(BigDecimal fbillunitprice) { this.fbillunitprice = fbillunitprice; }
    public BigDecimal getFbillqty() { return fbillqty; }
    public void setFbillqty(BigDecimal fbillqty) { this.fbillqty = fbillqty; }
    public BigDecimal getFinvoiceunitprice() { return finvoiceunitprice; }
    public void setFinvoiceunitprice(BigDecimal finvoiceunitprice) { this.finvoiceunitprice = finvoiceunitprice; }
    public BigDecimal getFinvoiceqty() { return finvoiceqty; }
    public void setFinvoiceqty(BigDecimal finvoiceqty) { this.finvoiceqty = finvoiceqty; }
    public LocalDateTime getFmatchtime() { return fmatchtime; }
    public void setFmatchtime(LocalDateTime fmatchtime) { this.fmatchtime = fmatchtime; }
}
