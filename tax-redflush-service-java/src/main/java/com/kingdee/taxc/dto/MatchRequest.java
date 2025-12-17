package com.kingdee.taxc.dto;

public class MatchRequest {
    private Long negativeApplyId;
    private String buyerTaxNo;
    private String sellerTaxNo;
    private String productCode;

    public Long getNegativeApplyId() { return negativeApplyId; }
    public void setNegativeApplyId(Long negativeApplyId) { this.negativeApplyId = negativeApplyId; }
    public String getBuyerTaxNo() { return buyerTaxNo; }
    public void setBuyerTaxNo(String buyerTaxNo) { this.buyerTaxNo = buyerTaxNo; }
    public String getSellerTaxNo() { return sellerTaxNo; }
    public void setSellerTaxNo(String sellerTaxNo) { this.sellerTaxNo = sellerTaxNo; }
    public String getProductCode() { return productCode; }
    public void setProductCode(String productCode) { this.productCode = productCode; }
}
