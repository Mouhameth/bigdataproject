import hive.HiveCartography;
import hive.HiveCountry;
import hive.HiveSociete;
import hive.HiveTour;
import org.apache.spark.sql.SparkSession;

public class MainHdfsToHiveTable {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("bigdataproject")
                .config("spark.master", "local")
                .enableHiveSupport()
                .getOrCreate();

        String hdfsPath = "hdfs://localhost:9000/tmp/hadoop-macbookpro/dfs/data/";

        HiveSociete.hdfsToHiveTable(spark,hdfsPath);
        HiveCartography.hdfsToHiveTable(spark,hdfsPath);
        HiveTour.hdfsToHiveTable(spark,hdfsPath);
        HiveCountry.hdfsToHiveTable(spark, hdfsPath);

    }
}
