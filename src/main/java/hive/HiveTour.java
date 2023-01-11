package hive;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HiveTour {
    public  static  void hdfsToHiveTable(SparkSession spark, String hdfsPath){
        Dataset<Row> df = spark.read().parquet(hdfsPath+"tours.parquet");
        df.write().format("hive").saveAsTable("t_tours");
    }
}
