package com.kingdee.taxc.mapper;

import com.kingdee.taxc.entity.MatchBill1201;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface MatchBill1201Mapper {
    MatchBill1201 selectById(@Param("fid") Long fid);
}
