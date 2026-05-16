import com.fasterxml.jackson.databind.ObjectMapper;
import com.settlement.functions.MockOrderGenerator;
import com.settlement.functions.MockPaymentGenerator;
import com.settlement.functions.TradeMatchingFunction;
import com.settlement.models.BankPayment;
import com.settlement.models.SettledTrade;
import com.settlement.models.TradeOrder;
import com.settlement.sinks.DematAccountSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.time.Duration;

public class TradeSettlementApp {
    public static void main(String[] args) throws Exception {
        boolean useKafka = Boolean.parseBoolean(
                System.getProperty(
                        "useKafka",
                        System.getenv().getOrDefault("USE_KAFKA", "true")
                )
        );
        DataStream<TradeOrder> orders;
        DataStream<BankPayment> payments;
// Tell Flink to start a local Web Server on port 8081
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
//        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStream<TradeOrder> orders=env.fromElements(
//                new TradeOrder("101","silver_etf"),
//                new TradeOrder("102","quant elss")
//        );

//        DataStream<BankPayment> payments = env.fromElements(
//                new BankPayment("101", "SUCCESS")
//        );
//        DataStream<BankPayment> payments = env.addSource(new MockPaymentGenerator());
//        DataStream<TradeOrder> orders = env.addSource(new MockOrderGenerator());

        WatermarkStrategy<TradeOrder> orderWatermarkStrategy = WatermarkStrategy
                .<TradeOrder>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event, timestamp) -> event.getEventTimestamp());

        WatermarkStrategy<BankPayment> paymentWatermarkStrategy = WatermarkStrategy
                .<BankPayment>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event, timestamp) -> event.getEventTimestamp());

        if (useKafka) {
            System.out.println("🚀 Booting up in KAFKA Mode...");

            // 1. Build the Kafka Connections
            KafkaSource<String> kafkaOrderSource = KafkaSource.<String>builder()
                    .setBootstrapServers("localhost:9092")
                    .setTopics("orders-topic")
                    .setGroupId("flink-engine")
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            KafkaSource<String> kafkaPaymentSource = KafkaSource.<String>builder()
                    .setBootstrapServers("localhost:9092")
                    .setTopics("payments-topic")
                    .setGroupId("flink-engine")
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            ObjectMapper mapper = new ObjectMapper();

            // 2. Attach Kafka to the Pipeline
            orders = env.fromSource(kafkaOrderSource, WatermarkStrategy.noWatermarks(), "Kafka Orders")
                    .map(json -> mapper.readValue(json, TradeOrder.class))
                    .assignTimestampsAndWatermarks(orderWatermarkStrategy);

            payments = env.fromSource(kafkaPaymentSource, WatermarkStrategy.noWatermarks(), "Kafka Payments")
                    .map(json -> mapper.readValue(json, BankPayment.class))
                    .assignTimestampsAndWatermarks(paymentWatermarkStrategy);

        } else {
            System.out.println("Booting up in MOCK Mode...");
            orders = env.addSource(new MockOrderGenerator())
                    .assignTimestampsAndWatermarks(orderWatermarkStrategy);

            payments = env.addSource(new MockPaymentGenerator())
                    .assignTimestampsAndWatermarks(paymentWatermarkStrategy);
        }

        DataStream<SettledTrade> completedTrades = orders.keyBy(TradeOrder::getOrderId)
                .connect(payments.keyBy(BankPayment::getOrderId))
                .process(new TradeMatchingFunction());

        completedTrades.addSink(new DematAccountSink());
        env.execute("Trade Settlement Pipeline Sandbox");
    }
}
