package producers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.*;
import schemas.Societe;

import java.util.Properties;

public class SocieteProducer {
    public static void produce(SparkSession spark, Properties props){

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        String filePath = "data/societe.csv";
        Dataset<Row> societeDf = spark.read().option("header", "true").csv(filePath);

        Encoder<Societe> encoder = Encoders.bean(Societe.class);

        Dataset<Societe> socDf = new Dataset<Societe>(spark, societeDf.logicalPlan(), encoder);
        Societe[] societes = (Societe[]) socDf.collect();

        int i = 0;

        System.out.println("societe producer is created ...");
        for (Societe societeObject : societes) {
            producer.send(new ProducerRecord<String, String>("societe",societeObject.getName(),societeObject.getPermalink()+","+societeObject.getName()+","+societeObject.getHomepage_url()+","+societeObject.getCategory_list()+","+ societeObject.getStatus()+","+societeObject.getCountry_code()+","+societeObject.getState_code()+","+societeObject.getRegion()+","+societeObject.getCity()+","+societeObject.getFounded_at()));
            i++;
        }
        System.out.println("Producer has sent "+i+" societes records successfully...");
        producer.close();
    }

}
