package com.lzc.solution.utils;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.math.BigDecimal;
import java.time.LocalDate;

public class JdbcSinkUtil {
    private static final String jdbcUrl = "jdbc:mysql://localhost:3306/flink?serverTimezone=Asia/Shanghai";
    private static final String username = "root";
    private static final String password = "123456";

    public static void writeToMySQL(DataStream<Tuple4<Integer, BigDecimal, LocalDate, Integer>> stream) {
        String insertQuery = "INSERT INTO result_t (l_orderkey, revenue, o_orderdate, o_shippriority) VALUES (?, ?, ?, ?)";

        stream.addSink(JdbcSink.sink(
                insertQuery,
                (ps, t) -> {
                    ps.setInt(1, t.f0); // l_orderkey
                    ps.setBigDecimal(2, t.f1); // total_revenue
                    ps.setDate(3, java.sql.Date.valueOf(t.f2)); // o_orderdate
                    ps.setInt(4, t.f3); // o_shippriority
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000) // 每次批量插入的大小
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(username)
                        .withPassword(password)
                        .build()
        ));
    }
}