package com.lzc.solution.objects;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDate;

@Getter
@Setter
public class Result {
    private int l_orderkey;
    private BigDecimal revenue;
    private LocalDate o_orderdate;
    private int o_shippriority;
}
