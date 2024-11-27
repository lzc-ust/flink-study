package com.lzc.solution.objects;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;

@Data
public class Orders {
    private int o_orderkey;
    private int o_custkey;
    private String o_orderstatus;
    private BigDecimal o_totalprice;
    private LocalDate o_orderdate;
    private String o_orderpriority;
    private String o_clerk;
    private int o_shippriority;
    private String o_comment;
}
