package com.kingdee.taxc.mapper;

import com.kingdee.taxc.entity.MatchBillItem1201;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface MatchBillItem1201Mapper {
    MatchBillItem1201 selectByEntryId(@Param("fentryid") Long fentryid);
    java.util.List<String> listProductCodesByBillId(@Param("fid") Long fid);
    java.util.List<com.kingdee.taxc.entity.MatchBillItem1201> listByBillId(@Param("fid") Long fid);
}
