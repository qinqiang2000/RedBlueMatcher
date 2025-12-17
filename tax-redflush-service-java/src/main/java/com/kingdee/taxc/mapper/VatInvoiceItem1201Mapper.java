package com.kingdee.taxc.mapper;

import com.kingdee.taxc.entity.VatInvoiceItem1201;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface VatInvoiceItem1201Mapper {
    VatInvoiceItem1201 selectByEntryId(@Param("fentryid") Long fentryid);
}
