import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.SparkSession;
import producers.CartographieProducer;
import producers.CountryProducer;
import producers.SocieteProducer;
import producers.ToursProducer;
import utils.JsonSerializer;
import java.util.Properties;

public class MainProducer {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                          .appName("bigdataproject")
                                          .config("spark.master", "local")
                                          .getOrCreate();


        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG,"big-data");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        SocieteProducer.produce(spark, props);
        CartographieProducer.produce(spark, props);
        ToursProducer.produce(spark, props);
        CountryProducer.produce(spark, props);

    }
}
