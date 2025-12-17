package com.kingdee.taxc.entity;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class MatchBillItem1201 {
    private Long fid;
    private Long fentryid;
    private Integer fseq;
    private String fgoodsname;
    private String fspbm;
    private String fspecification;
    private String funit;
    private BigDecimal fnum;
    private String ftaxrate;
    private BigDecimal funitprice;
    private BigDecimal famount;
    private BigDecimal ftax;
    private BigDecimal ftaxunitprice;
    private String fgoodscode;
    private String fitemmatchstatus;
    private BigDecimal fmatchtaxdeviation;

    public Long getFid() { return fid; }
    public void setFid(Long fid) { this.fid = fid; }
    public Long getFentryid() { return fentryid; }
    public void setFentryid(Long fentryid) { this.fentryid = fentryid; }
    public Integer getFseq() { return fseq; }
    public void setFseq(Integer fseq) { this.fseq = fseq; }
    public String getFgoodsname() { return fgoodsname; }
    public void setFgoodsname(String fgoodsname) { this.fgoodsname = fgoodsname; }
    public String getFspbm() { return fspbm; }
    public void setFspbm(String fspbm) { this.fspbm = fspbm; }
    public String getFspecification() { return fspecification; }
    public void setFspecification(String fspecification) { this.fspecification = fspecification; }
    public String getFunit() { return funit; }
    public void setFunit(String funit) { this.funit = funit; }
    public BigDecimal getFnum() { return fnum; }
    public void setFnum(BigDecimal fnum) { this.fnum = fnum; }
    public String getFtaxrate() { return ftaxrate; }
    public void setFtaxrate(String ftaxrate) { this.ftaxrate = ftaxrate; }
    public BigDecimal getFunitprice() { return funitprice; }
    public void setFunitprice(BigDecimal funitprice) { this.funitprice = funitprice; }
    public BigDecimal getFamount() { return famount; }
    public void setFamount(BigDecimal famount) { this.famount = famount; }
    public BigDecimal getFtax() { return ftax; }
    public void setFtax(BigDecimal ftax) { this.ftax = ftax; }
    public BigDecimal getFtaxunitprice() { return ftaxunitprice; }
    public void setFtaxunitprice(BigDecimal ftaxunitprice) { this.ftaxunitprice = ftaxunitprice; }
    public String getFgoodscode() { return fgoodscode; }
    public void setFgoodscode(String fgoodscode) { this.fgoodscode = fgoodscode; }
    public String getFitemmatchstatus() { return fitemmatchstatus; }
    public void setFitemmatchstatus(String fitemmatchstatus) { this.fitemmatchstatus = fitemmatchstatus; }
    public BigDecimal getFmatchtaxdeviation() { return fmatchtaxdeviation; }
    public void setFmatchtaxdeviation(BigDecimal fmatchtaxdeviation) { this.fmatchtaxdeviation = fmatchtaxdeviation; }
}
