package consumers;

import org.apache.spark.sql.*;
import schemas.Cartographie;
import java.util.ArrayList;
import java.util.List;

public class CartographieConsumer {
    public static void consume(SparkSession spark){
        Dataset<Row> df = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "cartographie")
                .option("startingOffsets", "earliest")
                .load();

        Dataset<String> indivValues = df
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

        //create new columns, parse out the orig message and fill column with the values
        Dataset<Row> values = indivValues
                .selectExpr("value",
                        "split(value,',')[0] as liste_categorie",
                        "split(value,',')[1] as automobile_et_sports",
                        "split(value,',')[2] as blancs",
                        "split(value,',')[3] as technologies_propres_ou_semi_conducteurs",
                        "split(value,',')[4] as divertissement",
                        "split(value,',')[5] as sante",
                        "split(value,',')[6] as fabrication",
                        "split(value,',')[7] as media",
                        "split(value,',')[8] as recherche_et_messagerie",
                        "split(value,',')[9] as autres")
                .drop("value");

        Encoder<Cartographie> encoder = Encoders.bean(Cartographie.class);

        Dataset<Cartographie> pjClass = new Dataset<Cartographie>(spark, values.logicalPlan(), encoder);
        pjClass.show();
        Cartographie[] cartographies = (Cartographie[]) pjClass.collect();

        System.out.println("Start cartographie consuming ...");

        int i = 0;

        String hdfsPath = "hdfs://localhost:9000/tmp/hadoop-macbookpro/dfs/data/cartographie.parquet";

        List<Cartographie> cartographieToHdfs = new ArrayList<>();

        for (Cartographie cartographie : cartographies ) {
            String liste_categorie = cartographie.getListe_categorie().substring(1, cartographie.getListe_categorie().length());

            String autres = cartographie.getAutres().substring(0, cartographie.getAutres().length()-1);
            cartographie.setListe_categorie(liste_categorie);
            cartographie.setAutres(autres);
            cartographieToHdfs.add(cartographie);
            i++;
        }

        Dataset<Cartographie> data=spark.createDataset(cartographieToHdfs,Encoders.bean(Cartographie.class));
        data.write().mode("overwrite").parquet(hdfsPath);
        System.out.println("Finish consuming "+i+" cartographie");
    }
}
