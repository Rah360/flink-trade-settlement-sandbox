package com.settlement.sinks;

import com.settlement.models.SettledTrade;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DematAccountSink extends RichSinkFunction<SettledTrade> {

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("imagine that we opened connection to Demat Postgres DB.");
    }

    @Override
    public void invoke(SettledTrade trade, Context context) throws Exception {
        // This runs EVERY TIME a SettledTrade falls out of the pipeline.

        if (trade.getPaymentStatus().equals("SUCCESS")) {
            System.out.println("[DEMAT ACCOUNT] Safely stored " + trade.getAsset() + " for Order ID: " + trade.getOrderId());
        } else {
            System.out.println("[DEMAT ACCOUNT] Rejected " + trade.getAsset() + " (Payment Failed).");
        }
    }

    @Override
    public void close() throws Exception {
        // This runs when the server shuts down.
        System.out.println("[DATABASE] Closed Demat DB connection.");
    }
}