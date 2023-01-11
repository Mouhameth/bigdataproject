import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MainHQL {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("bigdataproject")
                .config("spark.master", "local")
                .enableHiveSupport()
                .getOrCreate();

        // Partie 1: exploration, compréhension et nettoyage de la donnée

        //Question 1: quel est le nombre de sociétés différentes présentes dans le dataframe toursDf?
        spark.sql("SELECT count( distinct lien_societe) as nbSocietesIntoursDf FROM t_tours").show();

        //Question 2: quel est le nombre de sociétés différentes présentes dans le dataframe societesDf?
        spark.sql("SELECT count(*) as nbSocietesInSocieteDf FROM t_societe").show();

        //Question 3: dans le dataframe societesDf, quelle colonne peut être utilisé comme clé unique pour chaque société. Donnez le nom de la colonne.
        /*
        Dans societeDf la colonne permalink peut être utilisée comme clé unique
        */

        // Question 4: y a-t-il des sociétés dans le dataframe toursDf qui ne sont pas présentes dans le dataframe societesDf ?
        spark.sql("SELECT t.lien_societe as SocietesNotExistsInSocieteDf FROM t_tours t left join t_societe s on t.lien_societe = s.permalink where s.permalink is null").show();
        //Oui ça existe.

        // Question 5: fusionner les deux dataframes afin que toutes les colonnes du dataframe societesDf soient ajoutées au dataframe toursDf.
        // Nommez le nouveau dataframe obtenu mergedDf. Combien d'observations sont présentes dans mergedDf ?
        Dataset<Row> mergedDf =  spark.sql("SELECT * FROM t_tours join t_societe on t_tours.lien_societe = t_societe.permalink").toDF();
        mergedDf.show();

        // Partie 2: analyse des pays

        // Question 1: quels sont les sept premiers pays qui ont reçu l’investissement
        //total le plus élevé (dans tous les secteurs pour le type d'investissement
        //choisi)
        // &
        //Question 2: pour le type d'investissement choisi, créez un dataframe nommé
        //top7CountriesDf avec les sept premiers pays (en fonction du montant total
        //d'investissement reçu par chaque pays).
       Dataset<Row> top7CountriesDf = spark.sql("SELECT s.country_code, sum(t.montant_investi_en_euro) as somme from t_tours t, t_societe s where t.lien_societe = s.permalink and t.type_tour_investissement like 'venture' group by s.country_code order by somme desc limit 7 ").toDF();
       top7CountriesDf.show();

       //Question 3: identifiez les trois premiers pays anglophones dans le dataframe top7CountriesDf.
        // USA, GBR, INDIA

        // Partie 3: analyse des secteurs

        // Question 1: extraire le secteur primaire de chaque liste de catégories de la
        //colonne liste_categorie.
        Dataset<Row> listCategorie = spark.sql("select distinct category_list from t_societe").toDF();
        listCategorie.show();

        //Question 2: Utilisez le dataframe cartoDf (fichier cartographie.csv) pour
        //mapper chaque secteur primaire à l'un des huit secteurs principaux (« autres » peut également être considéré comme étant un secteur principal). –

    }
}