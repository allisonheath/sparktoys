import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

object MafExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MAF Example")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .getOrCreate()
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .option("delimiter", "\t")
      .option("comment", "#")
      .load("TCGA.ACC.mutect.abbe72a5-cb39-48e4-8df5-5fd2349f2bb2.somatic.maf")

    df.createOrReplaceTempView("mutations")

    val topTwenty = spark.sql("SELECT Hugo_Symbol, count(*) FROM mutations GROUP BY Hugo_symbol ORDER BY count(*) DESC LIMIT 20")

    val topTwentyMissense = spark.sql("SELECT Hugo_Symbol, count(*) FROM mutations WHERE Variant_Classification='Missense_Mutation' GROUP BY Hugo_symbol ORDER BY count(*) DESC LIMIT 20")

    val fat4 = spark.sql("SELECT Chromosome, Start_Position, End_Position, Strand, Variant_Classification, Variant_Type, Tumor_Sample_Barcode FROM mutations WHERE Hugo_Symbol='FAT4'")

    topTwenty.coalesce(1).write.format("com.databricks.spark.csv").save("results/topTwenty")

    topTwentyMissense.coalesce(1).write.format("com.databricks.spark.csv").save("results/topTwentyMissense")

    fat4.coalesce(1).write.format("com.databricks.spark.csv").save("results/fat4")
  }
}
