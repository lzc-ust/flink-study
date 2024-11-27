package com.lzc.solution.process;

import com.lzc.solution.objects.Customer;
import com.lzc.solution.objects.Orders;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class CustomerOrderProcessFunction extends KeyedCoProcessFunction<Integer, Customer, Orders, Tuple2<Customer, Orders>> {
    private final String segment; // 存储要匹配的市场细分
    private ValueState<Customer> customerState; // 用于存储客户数据

    public CustomerOrderProcessFunction(String segment) {
        this.segment = segment; // 初始化市场细分
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化状态
        ValueStateDescriptor<Customer> descriptor =
                new ValueStateDescriptor<>("customerState", Types.GENERIC(Customer.class));
        customerState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement1(Customer customer, Context ctx, Collector<Tuple2<Customer, Orders>> out) throws Exception {
        // 处理来自 customerStream 的元素
        // 检查市场细分
        if (segment.equals(customer.getC_mktsegment())) {
            // 更新状态
            customerState.update(customer);
        }
    }

    @Override
    public void processElement2(Orders order, Context ctx, Collector<Tuple2<Customer, Orders>> out) throws Exception {
        // 处理来自 orderStream 的元素
        Customer customer = customerState.value();

        // 检查是否有对应的客户
        if (customer != null && order.getO_custkey() == customer.getC_custkey()) {
            // 检查市场细分
            if (segment.equals(customer.getC_mktsegment())) {
                out.collect(new Tuple2<>(customer, order)); // 输出组合的结果
            }
        }
    }
}
