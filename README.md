If you do not need to execute the validation logic, just print the processing results in the console, and there is no need to deploy and configure the MYSQL-related parts.
Comment out JdbcSinkUtil.writeToMySQL(aggregatedStream); in FlinkStreamProcessing.java, and the program can run directly.

If you need to execute the validation logic, you need to configure the correct data source connection in mybatis-config.xml and JdbcSinkUtil.java.
The relevant verification logic is defined in src/main/java/com/lzc/solution/process/ResultVerification.java.

```MySQL
CREATE TABLE `customer` (
  `C_CUSTKEY` int(11) NOT NULL,
  `C_NAME` varchar(25) NOT NULL,
  `C_ADDRESS` varchar(40) NOT NULL,
  `C_NATIONKEY` int(11) NOT NULL,
  `C_PHONE` char(15) NOT NULL,
  `C_ACCTBAL` decimal(15,2) NOT NULL,
  `C_MKTSEGMENT` char(10) NOT NULL,
  `C_COMMENT` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`C_CUSTKEY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `orders` (
  `O_ORDERKEY` int(11) NOT NULL,
  `O_CUSTKEY` int(11) NOT NULL,
  `O_ORDERSTATUS` char(1) NOT NULL,
  `O_TOTALPRICE` decimal(15,2) NOT NULL,
  `O_ORDERDATE` date NOT NULL,
  `O_ORDERPRIORITY` char(15) NOT NULL,
  `O_CLERK` char(15) NOT NULL,
  `O_SHIPPRIORITY` int(11) NOT NULL,
  `O_COMMENT` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`O_ORDERKEY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `lineitem` (
  `L_ORDERKEY` int(11) NOT NULL,
  `L_PARTKEY` int(11) NOT NULL,
  `L_SUPPKEY` int(11) NOT NULL,
  `L_LINENUMBER` int(11) NOT NULL,
  `L_QUANTITY` decimal(15,2) NOT NULL,
  `L_EXTENDEDPRICE` decimal(15,2) NOT NULL,
  `L_DISCOUNT` decimal(15,2) NOT NULL,
  `L_TAX` decimal(15,2) NOT NULL,
  `L_RETURNFLAG` char(1) NOT NULL,
  `L_LINESTATUS` char(1) NOT NULL,
  `L_SHIPDATE` date NOT NULL,
  `L_COMMITDATE` date NOT NULL,
  `L_RECEIPTDATE` date NOT NULL,
  `L_SHIPINSTRUCT` char(25) NOT NULL,
  `L_SHIPMODE` char(10) NOT NULL,
  `L_COMMENT` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`L_ORDERKEY`,`L_LINENUMBER`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `result_t` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `l_orderkey` int(11) NOT NULL DEFAULT '0',
  `revenue` decimal(16,4) DEFAULT NULL,
  `o_orderdate` date DEFAULT NULL,
  `o_shippriority` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=303 DEFAULT CHARSET=utf8;
```
Related SQL, tbl files are in the Resources directory
```sql
LOAD DATA LOCAL INFILE 'E:\\project\\data-source\\customer.tbl' INTO TABLE CUSTOMER FIELDS TERMINATED BY '|' LINES TERMINATED BY '\r\n';
LOAD DATA LOCAL INFILE 'E:\\project\\data-source\\orders.tbl' INTO TABLE ORDERS FIELDS TERMINATED BY '|' LINES TERMINATED BY '\r\n';
LOAD DATA LOCAL INFILE 'E:\\project\\data-source\\lineitem.tbl' INTO TABLE LINEITEM FIELDS TERMINATED BY '|' LINES TERMINATED BY '\r\n';

SELECT
    l.l_orderkey,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
    o.o_orderdate,
    o.o_shippriority
FROM
    customer c
    JOIN orders o ON c.c_custkey = o.o_custkey
    JOIN lineitem l ON l.l_orderkey = o.o_orderkey
WHERE
    c.c_mktsegment = 'AUTOMOBILE'
    AND o.o_orderdate < DATE '1995-03-13'
    AND l.l_shipdate > DATE '1995-03-13'
GROUP BY
    l.l_orderkey,
    o.o_orderdate,
    o.o_shippriority
ORDER BY
    revenue DESC,
    o.o_orderdate;
LIMIT 10

SELECT
    l_orderkey,
    MAX(revenue) AS max_revenue,
    o_orderdate,
    o_shippriority
FROM
    result_t
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    max_revenue DESC,
    o_orderdate;
LIMIT 10
```
