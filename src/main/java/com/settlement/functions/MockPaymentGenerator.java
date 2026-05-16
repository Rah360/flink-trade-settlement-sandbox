package com.settlement.functions;

import com.settlement.models.BankPayment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Random;

public class MockPaymentGenerator implements SourceFunction<BankPayment> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<BankPayment> ctx) throws Exception {
        Random random = new Random();

        while (isRunning) {
            // Pick a random ID between 1 and 20 so it collides with our orders!
            String randomId = String.valueOf(random.nextInt(20) + 1);

            // 80% chance of SUCCESS, 20% chance of INSUFFICIENT_FUNDS
            String status = random.nextInt(100) < 80 ? "SUCCESS" : "INSUFFICIENT_FUNDS";

            BankPayment payment = new BankPayment(randomId, status,System.currentTimeMillis());
            ctx.collect(payment);

            // Emit a new payment every 1 second
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}