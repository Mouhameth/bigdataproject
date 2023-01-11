package producers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.*;
import schemas.Cartographie;
import java.util.Properties;

public class CartographieProducer {

    public static void produce(SparkSession spark, Properties props){
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        String filePath = "data/cartographie.csv";
        Dataset<Row> cartoDf = spark.read().option("header", "true").csv(filePath);

        Encoder<Cartographie> encoder = Encoders.bean(Cartographie.class);

        Dataset<Cartographie> cartographieDf = new Dataset<Cartographie>(spark, cartoDf.logicalPlan(), encoder);
        Cartographie[] cartographies = (Cartographie[]) cartographieDf.collect();

        int i = 0;
        System.out.println("cartographie producer is created ...");
        for (Cartographie cartographieObject : cartographies) {
            System.out.println(cartographieObject.getListe_categorie());
            producer.send(new ProducerRecord<String, String>("cartographie",cartographieObject.getListe_categorie(),cartographieObject.getListe_categorie()+","+cartographieObject.getAutomobile_et_sports()+","+cartographieObject.getBlancs()+","+cartographieObject.getTechnologies_propres_ou_semi_conducteurs()+","+cartographieObject.getDivertissement()+","+cartographieObject.getSante()+","+cartographieObject.getFabrication()+","+cartographieObject.getMedia()+","+cartographieObject.getRecherche_et_messagerie()+","+ cartographieObject.getAutres()));
            i++;
        }
        System.out.println("Producer has sent "+i+" cartographie records successfully...");
        producer.close();
    }


}
