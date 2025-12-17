package com.kingdee.taxc.mapper;

import com.kingdee.taxc.entity.MatchTempSummary1201;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface MatchTempSummary1201Mapper {
    int insertBatch(@Param("list") List<MatchTempSummary1201> list);
    int clearByJob(@Param("jobid") Long jobid);
    List<MatchTempSummary1201> listByJobOrdered(@Param("jobid") Long jobid);
}
