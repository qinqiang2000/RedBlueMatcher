package com.kingdee.taxc.mapper;

import com.kingdee.taxc.entity.MatchResult1201;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface MatchResult1201Mapper {
    int insertBatch(@Param("list") List<MatchResult1201> list);
}
