package com.kingdee.taxc.mapper;

import com.kingdee.taxc.entity.VatInvoice1201;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface VatInvoice1201Mapper {
    VatInvoice1201 selectById(@Param("fid") Long fid);
}
