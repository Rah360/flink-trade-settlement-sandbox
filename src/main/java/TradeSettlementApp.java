import com.settlement.functions.MockOrderGenerator;
import com.settlement.functions.MockPaymentGenerator;
import com.settlement.functions.TradeMatchingFunction;
import com.settlement.models.BankPayment;
import com.settlement.models.TradeOrder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

public class TradeSettlementApp {
    public static void main(String[] args) throws Exception {

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
        DataStream<BankPayment> payments = env.addSource(new MockPaymentGenerator());
        DataStream<TradeOrder> orders = env.addSource(new MockOrderGenerator());

        orders.keyBy(TradeOrder::getOrderId)
                .connect(payments.keyBy(BankPayment::getOrderId))
                .process(new TradeMatchingFunction())
                .print();

        env.execute("Trade Settlement Pipeline Sandbox");
    }
}
