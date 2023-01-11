import consumers.CartographieConsumer;
import consumers.CountryConsumer;
import consumers.SocieteConsumer;
import consumers.ToursConsumer;
import org.apache.spark.sql.SparkSession;

public class MainConsumer {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("bigdataproject")
                .config("spark.master", "local")
                .getOrCreate();

        SocieteConsumer.consume(spark);
        CartographieConsumer.consume(spark);
        ToursConsumer.consume(spark);
        CountryConsumer.consume(spark);

    }
}