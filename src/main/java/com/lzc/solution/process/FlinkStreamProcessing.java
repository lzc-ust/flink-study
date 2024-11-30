package com.lzc.solution.process;

import com.lzc.solution.objects.Customer;
import com.lzc.solution.objects.LineItem;
import com.lzc.solution.objects.Orders;
import com.lzc.solution.utils.ExcelReader;
import com.lzc.solution.utils.JdbcSinkUtil;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

public class FlinkStreamProcessing {
    private static final String customerFilePath = "src/main/resources/excel/customer.xlsx";
    private static final String orderFilePath = "src/main/resources/excel/orders.xlsx";
    private static final String lineItemFilePath = "src/main/resources/excel/lineitem.xlsx";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Customer> customers = ExcelReader.readCustomersFromExcel(customerFilePath);
        List<Orders> orders = ExcelReader.readOrdersFromExcel(orderFilePath);
        List<LineItem> lineItems = ExcelReader.readLineItemsFromExcel(lineItemFilePath);

        DataStream<Customer> customerStream = env.fromCollection(customers);
        DataStream<Orders> orderStream = env.fromCollection(orders);
        DataStream<LineItem> lineItemStream = env.fromCollection(lineItems);

        // 过滤客户数据
        DataStream<Customer> filteredCustomerStream = customerStream
                .filter(customer -> "AUTOMOBILE".equals(customer.getC_mktsegment()));

        // 连接客户和订单数据流
        DataStream<Tuple2<Customer, Orders>> customerOrderStream = filteredCustomerStream
                .connect(orderStream)
                .keyBy(Customer::getC_custkey, Orders::getO_custkey)
                .process(new CustomerOrderProcessFunction("AUTOMOBILE"));

        // 连接订单和订单项数据流
        DataStream<Tuple3<Customer, Orders, LineItem>> customerOrderLineItemStream = customerOrderStream
                .connect(lineItemStream)
                .keyBy(tuple -> tuple.f1.getO_orderkey(), LineItem::getL_orderkey)
                .process(new CustomerOrderLineItemProcessFunction());

        // 过滤数据
        SingleOutputStreamOperator<Tuple3<Customer, Orders, LineItem>> filteredCustomerOrderLineItemStream = customerOrderLineItemStream
                .filter(tuple -> tuple.f1.getO_orderdate().isBefore(LocalDate.parse("1995-03-13")) &&
                        tuple.f2.getL_shipdate().isAfter(LocalDate.parse("1995-03-13")));

        // 分组和聚合数据
        SingleOutputStreamOperator<Tuple4<Integer, BigDecimal, LocalDate, Integer>> aggregatedStream =
                filteredCustomerOrderLineItemStream
                        .map(tuple -> {
                            Integer l_orderkey = tuple.f2.getL_orderkey();
                            BigDecimal revenue = tuple.f2.getL_extendedprice()
                                    .multiply(BigDecimal.ONE.subtract(tuple.f2.getL_discount()));
                            LocalDate o_orderdate = tuple.f1.getO_orderdate();
                            Integer o_shippriority = tuple.f1.getO_shippriority();
                            return new Tuple4<>(l_orderkey, revenue, o_orderdate, o_shippriority);
                        })
                        .returns(new TypeHint<Tuple4<Integer, BigDecimal, LocalDate, Integer>>() {
                        })
                        .keyBy(new KeySelector<Tuple4<Integer, BigDecimal, LocalDate, Integer>, Tuple3<Integer, LocalDate, Integer>>() {
                            @Override
                            public Tuple3<Integer, LocalDate, Integer> getKey(Tuple4<Integer, BigDecimal, LocalDate, Integer> value) {
                                return new Tuple3<>(value.f0, value.f2, value.f3);
                            }
                        })
                        .reduce((ReduceFunction<Tuple4<Integer, BigDecimal, LocalDate, Integer>>) (value1, value2) -> {
                            BigDecimal totalRevenue = value1.f1.add(value2.f1);
                            return new Tuple4<>(value1.f0, totalRevenue, value1.f2, value1.f3);
                        });

        // 使用 JdbcSinkUtil 将 aggregatedStream 写入 MySQL 数据库
        JdbcSinkUtil.writeToMySQL(aggregatedStream);

        // 输出结果
        aggregatedStream.print();

        // 执行程序
        env.execute("Flink Stream Processing");
    }
}
