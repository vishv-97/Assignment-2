import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._

object UDFUtils {
  // Define UDF to concatenate first_name and last_name
  val concatNamesUDF: UserDefinedFunction = udf((first_name: String, last_name: String) => s"$first_name $last_name")
}
