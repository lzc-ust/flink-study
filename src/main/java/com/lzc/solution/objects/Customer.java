package com.lzc.solution.objects;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

@Data
public class Customer {
    private int c_custkey;
    private String c_name;
    private String c_address;
    private int c_nationkey;
    private String c_phone;
    private BigDecimal c_acctbal;
    private String c_mktsegment;
    private String c_comment;
}
