package com.settlement.functions;

import com.settlement.models.BankPayment;
import com.settlement.models.SettledTrade;
import com.settlement.models.TradeOrder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.flink.configuration.Configuration;

public class TradeMatchingFunction extends KeyedCoProcessFunction<String, TradeOrder, BankPayment, SettledTrade>{
    private ValueState<TradeOrder> savedOrderState;
    private ValueState<BankPayment> savedPaymentState;
    private long tenSecondsInMillis = 10 * 1000;
    //    who calls it? The TaskManager (the physical server).
    //    when is it called? Exactly once, right after the TaskManager assigns this code to a CPU slot,
    //    but before the data stream is turned on.
//    this is where we connect to database
    @Override
    public void open(Configuration parameters) {
//        this is like defining a table in rocksdb
        ValueStateDescriptor<TradeOrder> orderDescriptor =
                new ValueStateDescriptor<>("trade-order-state", TradeOrder.class);
        savedOrderState = getRuntimeContext().getState(orderDescriptor);

        ValueStateDescriptor<BankPayment> paymentDescriptor =
                new ValueStateDescriptor<>("bank-payment-state", BankPayment.class);
        savedPaymentState = getRuntimeContext().getState(paymentDescriptor);
    }

    @Override
    public void processElement1(TradeOrder order, KeyedCoProcessFunction<String, TradeOrder, BankPayment, SettledTrade>.Context ctx, Collector<SettledTrade> out) throws Exception {
        BankPayment savedPayment = savedPaymentState.value();
        if(savedPayment != null){
            out.collect(new SettledTrade(order.getOrderId(), order.getAsset(), savedPayment.getStatus()));
        savedPaymentState.clear();
        }else {
            savedOrderState.update(order);
            long deadline = order.getEventTimestamp() + tenSecondsInMillis;
            ctx.timerService().registerEventTimeTimer(deadline);
        }
    }

    @Override
    public void processElement2(BankPayment payment, KeyedCoProcessFunction<String, TradeOrder, BankPayment, SettledTrade>.Context ctx, Collector<SettledTrade> out) throws Exception {
        TradeOrder savedOrder = savedOrderState.value();
        if (savedOrder != null) {
            out.collect(new SettledTrade(payment.getOrderId(), savedOrder.getAsset(), payment.getStatus()));
            savedOrderState.clear();
        } else {
            savedPaymentState.update(payment);

            long deadline = payment.getEventTimestamp() + tenSecondsInMillis;
            ctx.timerService().registerEventTimeTimer(deadline);
        }
    }
//    a Priority Queue
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SettledTrade> out) throws Exception {
        TradeOrder stuckOrder = savedOrderState.value();
        BankPayment stuckPayment = savedPaymentState.value();
        System.out.println("⏰ Logical Watermark Clock hit: " + timestamp);
        if (stuckOrder != null) {
            System.err.println(" TIMEOUT: Order " + stuckOrder.getOrderId() + " expired. Payment never arrived.");
            savedOrderState.clear(); // free the memory!
        }

        if (stuckPayment != null) {
            System.err.println("TIMEOUT: Payment " + stuckPayment.getOrderId() + " expired. Order never arrived.");
            savedPaymentState.clear(); // free the memory!
        }
    }
}
