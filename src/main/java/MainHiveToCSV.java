import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class MainHiveToCSV {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("bigdataproject")
                .config("spark.master", "local")
                .enableHiveSupport()
                .getOrCreate();

        String path = "/Users/macbookpro/Documents/M2 BI/Veille technologique/outputs/";

        Dataset<Row> societe = spark.sql("select * from t_societe").toDF();
        societe.write().option("header",true)
                .option("sep",",")
                .mode(SaveMode.Overwrite)
                .csv(path+"societe");

        Dataset<Row> tours = spark.sql("select * from t_tours").toDF();
        tours.write().option("header",true)
                .option("sep",",")
                .mode(SaveMode.Overwrite)
                .csv(path+"tours");

        Dataset<Row> carto = spark.sql("select * from t_carto").toDF();
        carto.write().option("header",true)
                .option("sep",",")
                .mode(SaveMode.Overwrite)
                .csv(path+"cartographie");

        Dataset<Row> pays = spark.sql("select * from param_pays").toDF();
        carto.write().option("header",true)
                .option("sep",",")
                .mode(SaveMode.Overwrite)
                .csv(path+"pays");
    }
}
