package com.kingdee.taxc.dto;

import java.util.List;

public class MatchResult {
    private String buyerTaxNo;
    private String sellerTaxNo;
    private String productCode;
    private List<MatchedInvoiceItem> items;

    public String getBuyerTaxNo() { return buyerTaxNo; }
    public void setBuyerTaxNo(String buyerTaxNo) { this.buyerTaxNo = buyerTaxNo; }
    public String getSellerTaxNo() { return sellerTaxNo; }
    public void setSellerTaxNo(String sellerTaxNo) { this.sellerTaxNo = sellerTaxNo; }
    public String getProductCode() { return productCode; }
    public void setProductCode(String productCode) { this.productCode = productCode; }
    public List<MatchedInvoiceItem> getItems() { return items; }
    public void setItems(List<MatchedInvoiceItem> items) { this.items = items; }
}
