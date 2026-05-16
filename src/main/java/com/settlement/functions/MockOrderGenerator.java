package com.settlement.functions;
import com.settlement.models.TradeOrder;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;

// SourceFunction is Flink's way of creating a custom data intake pipe
public class MockOrderGenerator implements SourceFunction<TradeOrder> {

    // A switch to turn the faucet on and off
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<TradeOrder> ctx) throws Exception {
        Random random = new Random();
        String[] assets = {"SILVER_ETF", "QUANT_ELSS", "GOLD_BOND", "NIFTY_50"};

        // This loop runs infinitely until you stop the program
        while (isRunning) {

            // Generate a random ID and pick a random asset
            String randomAsset = assets[random.nextInt(assets.length)];
            String randomId = String.valueOf(random.nextInt(20) + 1);
            // Create the order and emit it out the pipe
            TradeOrder newOrder = new TradeOrder(randomId, randomAsset);
            ctx.collect(newOrder);

            // Wait 2 seconds before sending the next one
            Thread.sleep(2000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}