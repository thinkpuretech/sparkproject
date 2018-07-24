import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object CallDrop {
   def main(args : Array[String]) = {
      val session = SparkSession.builder.appName("TopCallDrops").getOrCreate()
      val struct = StructType(StructField("location",IntegerType,true) :: StructField("call_duration",IntegerType,true)::StructField("phone_number",LongType,true) :: 
                   StructField("error_code",StringType,true) :: Nil)
      val df = session.read.format("csv").option("sep",",").schema(struct).load("CDR.csv")
      df.createOrReplaceTempView("calldrops")
      val result = session.sql("SELECT CAST(phone_number AS VARCHAR(20)) FROM " + 
                                "( SELECT phone_number,COUNT(*) dropcount FROM calldrops WHERE error_code='0x860F16' GROUP BY phone_number ORDER BY dropcount DESC LIMIT 10)")

      result.write.text("hdfs:///user/edureka_398472/calldropresult2")
   }
}
