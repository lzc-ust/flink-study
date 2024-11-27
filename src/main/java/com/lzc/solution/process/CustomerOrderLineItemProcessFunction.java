package com.lzc.solution.process;

import com.lzc.solution.objects.Customer;
import com.lzc.solution.objects.LineItem;
import com.lzc.solution.objects.Orders;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple3;

public class CustomerOrderLineItemProcessFunction extends KeyedCoProcessFunction<Integer, Tuple2<Customer, Orders>, LineItem, Tuple3<Customer, Orders, LineItem>> {
    private ValueState<Tuple2<Customer, Orders>> customerOrderState; // 用于存储客户和订单数据

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化状态
        ValueStateDescriptor<Tuple2<Customer, Orders>> descriptor =
                new ValueStateDescriptor<>("customerOrderState", Types.TUPLE(Types.GENERIC(Customer.class), Types.GENERIC(Orders.class)));
        customerOrderState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement1(Tuple2<Customer, Orders> customerOrder, Context ctx, Collector<Tuple3<Customer, Orders, LineItem>> out) throws Exception {
        // 将客户和订单数据存储到状态中
        customerOrderState.update(customerOrder);

        // 检查是否有匹配的 LineItem
        LineItem lineItem = getLineItemForOrder(customerOrder.f1.getO_orderkey());
        if (lineItem != null) {
            out.collect(new Tuple3<>(customerOrder.f0, customerOrder.f1, lineItem)); // 输出组合的结果
        }
    }

    @Override
    public void processElement2(LineItem lineItem, Context ctx, Collector<Tuple3<Customer, Orders, LineItem>> out) throws Exception {
        // 检查 LineItem 的 l_orderkey 是否与当前状态中的订单匹配
        Tuple2<Customer, Orders> customerOrder = customerOrderState.value();
        if (customerOrder != null && lineItem.getL_orderkey() == (customerOrder.f1.getO_orderkey())) {
            out.collect(new Tuple3<>(customerOrder.f0, customerOrder.f1, lineItem)); // 输出组合的结果
        }
    }

    private LineItem getLineItemForOrder(Integer orderKey) {
        // 这里可以实现一个方法来获取与订单匹配的 LineItem
        // 由于我们在 processElement2 中已经处理了 LineItem，所以此方法可以省略
        return null; // 这里只是占位，实际逻辑在 processElement2 中处理
    }
}
