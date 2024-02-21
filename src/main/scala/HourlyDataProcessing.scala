import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Calendar

object HourlyDataProcessing {
  import UDFUtils.concatNamesUDF  // Import the UDF
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("HourlyDataProcessing")
      .master("local[*]")
      .getOrCreate()

    // Get the current hour
    val currentHour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY)
    println(currentHour)

    // Read data for the current hour, modify folder name in resource folder according to current hour.
    val hourlyData: DataFrame = spark.read.format("csv")
      .option("header", true)
      .load(s"src/main/resources/$currentHour/transactions.csv")

    hourlyData.show(10)

    // performing filter operation
    val filteredHourlyData: DataFrame = hourlyData.filter(col("market").isin("Brazil", "France"))
    // show filtered data
    filteredHourlyData.show(5)

    // Reading new broadcast dataframe
    val carData: DataFrame = spark.read.format("csv")
      .option("header", true)
      .load(s"src/main/resources/car.csv")

    carData.show(10)

    // Perform a broadcast hash join based on "id" column
    val joinedData: DataFrame = filteredHourlyData.join(
      broadcast(carData),
      Seq("id"),
      "inner"
    )

    joinedData.show(10)

    // Apply the UDF and create a new column 'full_name'
    val processedData: DataFrame = joinedData.withColumn("full_name", concatNamesUDF(col("first_name"), col("last_name")))
      .drop("first_name", "last_name")

    processedData.show(10)

    // Define PostgreSQL connection properties
    val postgresProps = new java.util.Properties()
    postgresProps.setProperty("driver", "org.postgresql.Driver")
    // here we also need to define postgres username and password,but due to security reason removing that when uploading to github.
    

    // Define PostgreSQL connection URL
    val postgresUrl = "jdbc:postgresql://localhost:5432/postgres"

    // Save the final processed data into a PostgreSQL table
    processedData.write.mode("overwrite").jdbc(postgresUrl, "assignment_2", postgresProps)


    // Stop the Spark session
    spark.stop()
  }
}
