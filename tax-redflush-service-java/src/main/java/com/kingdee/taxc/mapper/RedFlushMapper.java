package com.kingdee.taxc.mapper;

import com.kingdee.taxc.dto.MatchedInvoiceItem;
import com.kingdee.taxc.dto.CandidateStat;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface RedFlushMapper {

    @Select(
            "select vii.fid as invoiceId, " +
            "       vii.fentryid as itemId, " +
            "       vii.fspbm as productCode, " +
            "       vii.fnum as quantity, " +
            "       vii.famount as amount, " +
            "       vii.funitprice as unitPrice " +
            "from t_sim_vatinvoice_item_1201 vii " +
            "where vii.fspbm = #{productCode} " +
            "  and exists (select 1 from t_sim_vatinvoice_1201 vi " +
            "              where vi.fid = vii.fid " +
            "                and vi.fbuyertaxno = #{buyerTaxNo} " +
            "                and vi.fsalertaxno = #{sellerTaxNo} " +
            "                and coalesce(vi.ftotalamount, 0) > 0) " +
            "order by vii.famount desc"
    )
    List<MatchedInvoiceItem> matchByTaxAndProduct(@Param("buyerTaxNo") String buyerTaxNo,
                                                  @Param("sellerTaxNo") String sellerTaxNo,
                                                  @Param("productCode") String productCode);

    @Select(
            "select count(*) as cnt, coalesce(sum(vii.famount),0) as sumAmount " +
            "from t_sim_vatinvoice_item_1201 vii " +
            "where vii.fspbm = #{productCode} " +
            "  and exists (select 1 from t_sim_vatinvoice_1201 vi " +
            "              where vi.fid = vii.fid " +
            "                and vi.fbuyertaxno = #{buyerTaxNo} " +
            "                and vi.fsalertaxno = #{sellerTaxNo} " +
            "                and coalesce(vi.ftotalamount, 0) > 0)"
    )
    CandidateStat statForProduct(@Param("buyerTaxNo") String buyerTaxNo,
                                 @Param("sellerTaxNo") String sellerTaxNo,
                                 @Param("productCode") String productCode);

    @Select(
            "<script> " +
            "select vii.fid as invoiceId, " +
            "       vii.fentryid as itemId, " +
            "       vii.fspbm as productCode, " +
            "       vii.fnum as quantity, " +
            "       vii.famount as amount, " +
            "       vii.funitprice as unitPrice " +
            "from t_sim_vatinvoice_item_1201 vii " +
            "where vii.fspbm = #{productCode} " +
            "  and exists (select 1 from t_sim_vatinvoice_1201 vi " +
            "              where vi.fid = vii.fid " +
            "                and vi.fbuyertaxno = #{buyerTaxNo} " +
            "                and vi.fsalertaxno = #{sellerTaxNo} " +
            "                and coalesce(vi.ftotalamount, 0) > 0) " +
            "  and vii.fid in " +
            "  <foreach item='id' collection='invoiceIds' open='(' separator=',' close=')'>#{id}</foreach> " +
            "order by vii.famount asc " +
            "</script>"
    )
    List<MatchedInvoiceItem> matchOnInvoices(@Param("buyerTaxNo") String buyerTaxNo,
                                             @Param("sellerTaxNo") String sellerTaxNo,
                                             @Param("productCode") String productCode,
                                             @Param("invoiceIds") List<Long> invoiceIds);

    @Select(
            "<script> " +
            "select vii.fid as invoiceId, " +
            "       vii.fentryid as itemId, " +
            "       vii.fgoodscode as productCode, " +
            "       vii.fnum as quantity, " +
            "       vii.famount as amount " +
            "from t_sim_vatinvoice_item_1201 vii " +
            "where vii.fgoodscode in " +
            "  <foreach item='code' collection='productCodes' open='(' separator=',' close=')'>#{code}</foreach> " +
            "  and exists (select 1 from t_sim_vatinvoice_1201 vi " +
            "              where vi.fid = vii.fid " +
            "                and vi.fbuyertaxno = #{buyerTaxNo} " +
            "                and vi.fsalertaxno = #{sellerTaxNo} " +
            "                and coalesce(vi.ftotalamount, 0) > 0) " +
            "</script>"
    )
    List<MatchedInvoiceItem> findPositiveByTaxAndProductCodes(@Param("buyerTaxNo") String buyerTaxNo,
                                                              @Param("sellerTaxNo") String sellerTaxNo,
                                                              @Param("productCodes") List<String> productCodes);
}
