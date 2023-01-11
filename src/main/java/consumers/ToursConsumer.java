package consumers;

import org.apache.spark.sql.*;
import schemas.Tour;
import java.util.ArrayList;
import java.util.List;

public class ToursConsumer {
    public static void consume(SparkSession spark){
        Dataset<Row> df = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "tours")
                .option("startingOffsets", "earliest")
                .load();

        Dataset<String> indivValues = df
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

        //create new columns, parse out the orig message and fill column with the values
        Dataset<Row> values = indivValues
                .selectExpr("value",
                        "split(value,',')[0] as lien_societe",
                        "split(value,',')[1] as lien_tour_investissement",
                        "split(value,',')[2] as type_tour_investissement",
                        "split(value,',')[3] as code_tour_investissement",
                        "split(value,',')[4] as investi_en",
                        "split(value,',')[5] as montant_investi_en_euro"
                        )
                .drop("value");

        Encoder<Tour> encoder = Encoders.bean(Tour.class);

        Dataset<Tour> pjClass = new Dataset<Tour>(spark, values.logicalPlan(), encoder);
        Tour[] tours = (Tour[]) pjClass.collect();

        System.out.println("Start tour consuming ...");
        int i = 0;

        String hdfsPath = "hdfs://localhost:9000/tmp/hadoop-macbookpro/dfs/data/tours.parquet";

        List<Tour> tourToHdfs = new ArrayList<>();

        for (Tour tour : tours) {
            String lienSociete = tour.getLien_societe().substring(1,tour.getLien_societe().length());
            lienSociete = lienSociete.toLowerCase();
            String montant_investi_en_euro = tour.getMontant_investi_en_euro().substring(0, tour.getMontant_investi_en_euro().length()-1);
            tour.setLien_societe(lienSociete);
            tour.setMontant_investi_en_euro(montant_investi_en_euro);
            tourToHdfs.add(tour);
            i++;
        }

        Dataset<Tour> data=spark.createDataset(tourToHdfs,Encoders.bean(Tour.class));
        data.write().mode("overwrite").parquet(hdfsPath);

        System.out.println("Finish consuming "+i+" tours");
    }
}
