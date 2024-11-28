package com.lzc.solution.dao;


import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.lzc.solution.objects.Result;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface ResultDao extends BaseMapper<Result> {

    List<Result> queryOrdersRevenue(@Param("orderDate") String orderDate, @Param("shipDate") String shipDate);

    List<Result> queryFlinkResult();
}