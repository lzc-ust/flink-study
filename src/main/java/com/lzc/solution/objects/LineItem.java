package com.lzc.solution.objects;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;

@Data
public class LineItem {
    private int l_orderkey;
    private int l_partkey;
    private int l_suppkey;
    private int l_linenumber;
    private BigDecimal l_quantity;
    private BigDecimal l_extendedprice;
    private BigDecimal l_discount;
    private BigDecimal l_tax;
    private String l_returnflag;
    private String l_linestatus;
    private LocalDate l_shipdate;
    private LocalDate l_commitdate;
    private LocalDate l_receiptdate;
    private String l_shipinstruct;
    private String l_shipmode;
    private String l_comment;
}
