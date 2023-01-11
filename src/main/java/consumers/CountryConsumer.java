package consumers;
import org.apache.spark.sql.*;
import schemas.Country;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;


public class CountryConsumer {
    public static void consume(SparkSession spark){
        System.out.println("Start countries consuming ...");
        Dataset<Row> df = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "countries")
                .option("startingOffsets", "earliest")
                .load();

       Dataset<String> indivValues = df
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

        //create new columns, parse out the orig message and fill column with the values
        Dataset<Row> values = indivValues
                .selectExpr("value",
                        "split(value,':')[0] as name")
                .drop("value");

        Encoder<Country> encoder = Encoders.bean(Country.class);

        Dataset<Country> pjClass = new Dataset<Country>(spark, values.logicalPlan(), encoder);
        System.out.println("Finish consuming "+pjClass.count()+" countries");
        String hdfsPath = "hdfs://localhost:9000/tmp/hadoop-macbookpro/dfs/data/pays.parquet";
        Country[] countries = (Country[]) pjClass.collect();

        int i =0;
        List<String> countriesToDf = new ArrayList<String>();
        for (Country country : countries) {
            String name = country.getName().substring(1, country.getName().length());
            name = name.substring(0, name.length() - 1);
            countriesToDf.add(name);
            i++;
        }
        System.out.println(i+" countries are consumed.");

        Dataset<String> data=spark.createDataset(countriesToDf,Encoders.STRING());
        Dataset<Row> dataToHdfs = data.selectExpr("value",
                        "split(value,':')[0] as name")
                .drop("value");
        dataToHdfs.write().mode("overwrite").parquet(hdfsPath);
    }
}