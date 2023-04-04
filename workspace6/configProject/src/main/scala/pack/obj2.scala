package pack

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.databricks.spark.avro._


object obj2 {


	def dfZipWithIndex(
			df: DataFrame,
			offset: Int = 1,
			colName: String = "id") : DataFrame = {
					df.sqlContext.createDataFrame(
							df.rdd.zipWithIndex.map(element =>
							Row.fromSeq(Seq(element._2 + offset) ++ element._1.toSeq)
									),
							StructType(
									Array(StructField(colName,LongType,false)) ++ df.schema.fields
									)
							) 
	}


	def main(args: Array[String]): Unit = {




			val conf = new SparkConf().setAppName("Gitpractice").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")


					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._


					val df = spark.read.format("csv").option("header","true")
					.load("file:///c://scenaridata/data55.csv")

					df.show()
					
					val df2 = dfZipWithIndex(df)
					df2.show()

					print("ashjjaskdn")
					
					val count = df.count()
					print(count)
					println
					val limit = 10
					
					val mod = count / limit
					println
					println(mod)
			
			
					val z = df2.withColumn("batchid", col("id") % mod + 1)
					
					z.show()
					
					

					
					val maxbatchid = spark.read.format("snowflake")
					.option("sfURL","https://eogjppo-wl54107.snowflakecomputing.com")
					.option("sfAccount","eogjppo")
					.option("sfUser","zeyobronanalytics66")
					.option("sfPassword","Zeyo@908")
					.option("sfDatabase","zeyodb")
					.option("sfSchema","PUBLIC")
					.option("sfRole","ACCOUNTADMIN")
					.option("sfWarehouse","COMPUTE_WH")
					.option("query","select max(id) as maxid,max(batchid) as maxbatch from zeyodb.public.sample2")
					.load()

					maxbatchid.show()

					val crossjoin = z.crossJoin(maxbatchid)

					crossjoin.show()

					val finaldf = crossjoin.withColumn("id", col("id")+col("MAXID"))
					.withColumn("batchid", col("batchid")+col("MAXBATCH"))
					.drop("MAXID","MAXBATCH")

					finaldf.show(100)




//					finaldf.write.format("snowflake")
//					.option("sfURL","https://eogjppo-wl54107.snowflakecomputing.com")
//					.option("sfAccount","eogjppo")
//					.option("sfUser","zeyobronanalytics66")
//					.option("sfPassword","Zeyo@908")
//					.option("sfDatabase","zeyodb")
//					.option("sfSchema","PUBLIC")
//					.option("sfRole","ACCOUNTADMIN")
//					.option("sfWarehouse","COMPUTE_WH")
//					.option("dbtable","sample2")
//					.mode("append")
//					.save()

	}		

}