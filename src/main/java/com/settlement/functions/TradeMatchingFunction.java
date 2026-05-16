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
    private ValueState<BankPayment> savedPaymentState;

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
    public void processElement1(TradeOrder order, KeyedCoProcessFunction<String, TradeOrder, BankPayment, String>.Context ctx, Collector<String> out) throws Exception {
        BankPayment savedPayment = savedPaymentState.value();
        if(savedPayment != null){
            out.collect("SUCCESS [ORDER LATE]: Settled " + order.getAsset() + " (Order " + order.getOrderId() + ") with status " + savedPayment.getStatus());
            savedPaymentState.clear();
        }else {
            savedOrderState.update(order);
            long tenSecondsInMillis = 10 * 1000;
            long deadline = ctx.timerService().currentProcessingTime() + tenSecondsInMillis;
            ctx.timerService().registerProcessingTimeTimer(deadline);
            out.collect("RECEIVED ORDER: " + order.getAsset() + " (ID: " + order.getOrderId() + "). Holding in memory. 10-second timer started...");
        }
    }

    @Override
    public void processElement2(BankPayment payment, KeyedCoProcessFunction<String, TradeOrder, BankPayment, String>.Context ctx, Collector<String> out) throws Exception {
        TradeOrder savedOrder = savedOrderState.value();
        if (savedOrder != null) {
            out.collect("SUCCESS: Settled " + savedOrder.getAsset() + " (Order " + savedOrder.getOrderId() + ")");
            savedOrderState.clear();
        } else {
            savedPaymentState.update(payment);
            long deadline = ctx.timerService().currentProcessingTime() + (10 * 1000);
            ctx.timerService().registerProcessingTimeTimer(deadline);
            out.collect("RECEIVED PAYMENT: Status " + payment.getStatus() + " (ID: " + payment.getOrderId() + "). Waiting for order...");
        }
    }
//    a Priority Queue
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        TradeOrder stuckOrder = savedOrderState.value();
        BankPayment stuckPayment = savedPaymentState.value();

        if (stuckOrder != null) {
            out.collect("TIMEOUT: Order " + stuckOrder.getOrderId() + " expired. Payment never arrived.");
            savedOrderState.clear(); // free the memory!
        }

        if (stuckPayment != null) {
            out.collect("TIMEOUT: Payment " + stuckPayment.getOrderId() + " expired. Order never arrived.");
            savedPaymentState.clear(); // free the memory!
        }
    }
}
