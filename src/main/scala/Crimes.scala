
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Crimes extends App {

  val spark = SparkSession
    .builder()
    .appName( "Crimes")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //Lecture du dataset
  val crimesDF = spark.read
    .option("inferSchema", "true")
    .option("header","true")
    .csv("src/main/resources/data/Crimes_-_2001_to_Present.csv")

  //crimesDF.show()


  //Renommage des colonnes (non obligatoire : ici on s'est simplement aperçu après coup qu'il ne fallait pas oublier
  // l'option header lors de la lecture du csv)
  val crimesWithColumnsNamesDF = crimesDF.toDF(
"ID", "Case_Number", "Date", "Block", "IUCR", "Primary_Type", "Description", "Location_Description",
    "Arrest", "Domestic", "Beat", "District", "Ward", "Community_Area", "FBI_Code", "X_Coordinate", "Y_Coordinate",
    "Year", "Updated_On", "Latitude", "Longitude", "Location", "Historical_Wards", "Zip_Codes", "Community_Areas",
    "Census_Tracts", "Wards", "Boundaries", "Police_Districts", "Police_Beats"
  )

  //Etudier le schéma du dataframe obtenu est important
  // crimesDF.printSchema()

  // 1. Identifier le type de crime ayant le plus d'arrestation à la clef

  val arrestDF = crimesDF.filter(col("Arrest")=== true)

  val crimesWithArrestsDF = arrestDF
    .groupBy("Primary Type")
    .agg(
      count("Primary Type").as("Number_of_crimes")
    )
    .orderBy(col("Number_of_crimes").desc_nulls_last)

  //crimesWithArrestsDF.show()
  /* notre 1er résultat nous montre que les crimes liés à la drogue arrivent très nettement en 1ère position sur la
  ville de Chicago
   */

  // 2. Montrer les arrestations / nb crimes par années

  val nbArrestDF = arrestDF
    .groupBy("Year")
    .agg(
      count("Arrest").as("Nb_Arrests"),
    )

  val nbCrimesDF = crimesDF
    .groupBy("Year")
    .agg(
      count("Case Number").as("Nb_Crimes"),
    )

  val nbArrestByCrimeByYearDF = nbArrestDF.join(nbCrimesDF, "Year").orderBy(col("Year").desc_nulls_last)
  //nbArrestByCrimeByYearDF.show()
  /* Il y a de moins en moins d'arrestations car il y a de moins en moins de crimes par an => le crime a baissé à Chicago
  depuis 20 ans
  On remarque également que peu de crimes mènent à une arrestation
   */

  // 3. Identifier le district le moins performant

  //Compter le nombre de districts
  crimesDF.select(countDistinct(col("District")))
  //à noter : un .show me donne 24, alors que les numéros de districts vont jusque 31

  val nbArrestByDistrictDF = arrestDF
    .groupBy("District")
    .agg(
      count("Arrest").as("Nb_Arrests"),
    )

  val nbCrimesByDistrictDF = crimesDF
    .groupBy("District")
    .agg(
      count("Case Number").as("Nb_Crimes"),
    )

  val nbArrestByCrimeByDistrictDF = nbArrestByDistrictDF.join(nbCrimesByDistrictDF, "District").orderBy(col("Nb_Arrests"))
  //nbArrestByCrimeByDistrictDF.show(24)
  /* on constate que le district 11 est celui qui compte le plus d'arrestations (car plus de crimes)
  et que les districts 21 et 31 semblent être très sûrs
  cependant, on peut affiner ce résultat en cherchant un taux d'arrestations
   */

  val arrestRateByDistrictDF = nbArrestByCrimeByDistrictDF
    .withColumn("Arrest_Rate", col("Nb_Arrests")/col("Nb_Crimes"))
    .orderBy(col("Arrest_Rate").desc_nulls_last)

  //arrestRateByDistrictDF.show(24)
  /* cette nouvelle colonne met en avant que c'est dans les districts les moins sûrs (=ceux dans lesquels il y a
  le plus grand nombre de crimes) que le taux d'arrestations est le plus élevé (+ de 40%)
  (on ignore bien évidemment le district 21 et son trop petit nombre de crimes)
   */

  // 4. Evolution de la criminalité de Chicago sur 20 ans (= criminalité par districts)

  val nbChicagoCrimesDF = crimesDF.select("Year", "District", "Case Number")
  val nbCountChicagoCrimesDF = nbChicagoCrimesDF
    .groupBy("Year", "District")
    .agg(
      count("Case Number").as("Nb_Crimes"),
    )
    .orderBy(col("Year").desc_nulls_last)

  //nbCountChicagoCrimesDF.show(100)

  val worstDistrictByYearDF = nbCountChicagoCrimesDF
    .groupBy("Year")
    .agg(
      max("Nb_Crimes").as("Worst_District")
    )
    .orderBy(col("Year").desc_nulls_last)

  //worstDistrictByYearDF.show()

  //on joint nos 2 df et on élimine les colonnes en doubles
  val worstDistrictByYearFinalDF = worstDistrictByYearDF.join(
    nbCountChicagoCrimesDF.withColumnRenamed("Year", "YearOfCrime"),
    worstDistrictByYearDF.col("Worst_District") === nbCountChicagoCrimesDF.col("Nb_Crimes"))

  val df = worstDistrictByYearFinalDF.drop("YearOfCrime", "Nb_Crimes")
  //df.orderBy(col("Year").desc_nulls_last).show()

  /* on constate que la criminalité s'est déplacée en 20 ans, en même temps qu'elle a baissée de manière globale,
   à partir de la fin des années 2000. Elle est passée du district 8 au district 11, et plus récemment c'est le
    district 6 qui a connu le plus de crimes sur l'année 2021.
   */



}
