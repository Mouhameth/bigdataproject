package consumers;

import org.apache.spark.sql.*;
import schemas.Societe;
import java.util.ArrayList;
import java.util.List;

public class SocieteConsumer {
    public static void consume(SparkSession spark){
        Dataset<Row> df = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "societe")
                .option("startingOffsets", "earliest")
                .load();

        Dataset<String> indivValues = df
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

        //create new columns, parse out the orig message and fill column with the values
        Dataset<Row> values = indivValues
                .selectExpr("value",
                        "split(value,',')[0] as permalink",
                        "split(value,',')[1] as name",
                        "split(value,',')[2] as homepage_url",
                        "split(value,',')[3] as category_list",
                        "split(value,',')[4] as status",
                        "split(value,',')[5] as country_code",
                        "split(value,',')[6] as state_code",
                        "split(value,',')[7] as region",
                        "split(value,',')[8] as city",
                        "split(value,',')[9] as founded_at")
                .drop("value");

        Encoder<Societe> encoder = Encoders.bean(Societe.class);

        Dataset<Societe> pjClass = new Dataset<Societe>(spark, values.logicalPlan(), encoder);
        Societe[] societes = (Societe[]) pjClass.collect();

        System.out.println("Start societe consuming ...");
        int i = 0;

        String hdfsPath = "hdfs://localhost:9000/tmp/hadoop-macbookpro/dfs/data/societe.parquet";

        List<Societe> societeToHdfs = new ArrayList<>();

        for (Societe societe : societes ) {
            String permalink = societe.getPermalink().substring(1, societe.getPermalink().length());
            permalink = permalink.toLowerCase();
            String founded_at = societe.getFounded_at().substring(0, societe.getFounded_at().length()-1);
            societe.setPermalink(permalink);
            societe.setFounded_at(founded_at);
            societeToHdfs.add(societe);
            i++;
        }

        Dataset<Societe> data=spark.createDataset(societeToHdfs,Encoders.bean(Societe.class));
        data.write().mode("overwrite").parquet(hdfsPath);

        System.out.println("Finish consuming "+i+" societe");

    }
}
