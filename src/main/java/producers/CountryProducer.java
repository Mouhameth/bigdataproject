package producers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.*;
import schemas.Country;
import java.util.Properties;

public class CountryProducer {

    public static void produce(SparkSession spark, Properties props){

        String filePath = "data/pays.csv";
        Dataset<Row> paysDf = spark.read().option("header", "true").csv(filePath);

        Dataset<Row> values = paysDf
                .selectExpr("pays",
                        "split(pays,':')[0] as name")
                .drop("pays");

        Encoder<Country> encoder = Encoders.bean(Country.class);

        Dataset<Country> coutriesDf = new Dataset<Country>(spark, values.logicalPlan(), encoder);
        Country[] countries = (Country[]) coutriesDf.collect();

        System.out.println("Start countries consuming ...");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        int i = 0;
        System.out.println("country producer is created ...");
        for (Country country : countries) {
            producer.send(new ProducerRecord<String, String>("countries",country.getName(),country.getName()));
            i++;
        }
        System.out.println("Producer has sent "+i+" countries records successfully...");
        producer.close();
    }
}
