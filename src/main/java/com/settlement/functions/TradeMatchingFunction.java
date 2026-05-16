package com.settlement.functions;

import com.settlement.models.BankPayment;
import com.settlement.models.TradeOrder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.flink.configuration.Configuration;

public class TradeMatchingFunction extends KeyedCoProcessFunction<String, TradeOrder, BankPayment, String>{
    private ValueState<TradeOrder> savedOrderState;

    //    who calls it? The TaskManager (the physical server).
    //    when is it called? Exactly once, right after the TaskManager assigns this code to a CPU slot,
    //    but before the data stream is turned on.
//    this is where we connect to database
    @Override
    public void open(Configuration parameters) {
//        this is like defining a table in rocksdb
        ValueStateDescriptor<TradeOrder> descriptor =
                new ValueStateDescriptor<>("trade-order-state", TradeOrder.class);
        savedOrderState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement1(TradeOrder order, KeyedCoProcessFunction<String, TradeOrder, BankPayment, String>.Context ctx, Collector<String> out) throws Exception {
        savedOrderState.update(order);
        long tenSecondsInMillis = 10 * 1000;
        long deadline = ctx.timerService().currentProcessingTime() + tenSecondsInMillis;
        ctx.timerService().registerProcessingTimeTimer(deadline);
        out.collect("RECEIVED ORDER: " + order.getAsset() + " (ID: " + order.getOrderId() + "). Holding in memory. 10-second timer started...");
    }

    @Override
    public void processElement2(BankPayment payment, KeyedCoProcessFunction<String, TradeOrder, BankPayment, String>.Context ctx, Collector<String> out) throws Exception {
        TradeOrder savedOrder = savedOrderState.value();

        if (savedOrder != null) {
            out.collect("SUCCESS: Settled " + savedOrder.getAsset() + " (Order " + savedOrder.getOrderId() + ")");

            savedOrderState.clear();

        } else {
            out.collect("WARNING: Payment received for ID " + payment.getOrderId() + " but no Order found.");
        }
    }
//    a Priority Queue
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        TradeOrder stuckOrder = savedOrderState.value();
        if (stuckOrder != null) {
            out.collect("TIMEOUT ALERT: Payment never arrived for " + stuckOrder.getAsset() + " (Order " + stuckOrder.getOrderId() + "). Cancelling Trade.");
            savedOrderState.clear();
        }
    }
}
