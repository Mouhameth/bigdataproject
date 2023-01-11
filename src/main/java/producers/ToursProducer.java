package producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.*;
import schemas.Tour;
import java.util.Properties;

public class ToursProducer {

    public static void produce(SparkSession spark, Properties props){
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        String filePath = "data/tours.csv";
        Dataset<Row> toursDf = spark.read().option("header", "true").csv(filePath);

        Encoder<Tour> encoder = Encoders.bean(Tour.class);

        Dataset<Tour> tDf = new Dataset<Tour>(spark, toursDf.logicalPlan(), encoder);
        Tour[] tours = (Tour[]) tDf.collect();

        int i = 0;

        System.out.println("tour producer is created ...");
        for (Tour tourObject : tours) {
           producer.send(new ProducerRecord<String, String>("tours",tourObject.getCode_tour_investissement(),tourObject.getLien_societe()+","+tourObject.getLien_tour_investissement()+","+tourObject.getType_tour_investissement()+","+tourObject.getCode_tour_investissement()+","+tourObject.getInvesti_en()+","+tourObject.getMontant_investi_en_euro()));
           i++;
        }
        System.out.println("Producer has sent "+i+" tours records successfully...");
        producer.close();
    }
}
