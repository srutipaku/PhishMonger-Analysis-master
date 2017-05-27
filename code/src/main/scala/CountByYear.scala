import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import java.io._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import java.io.File

object CountByYear {
case class data(phish_id: String, target_id: String, timestamp: String, submitter: String, submission_time: String, submission_url: String, submission_status: String, submission_verification_status: String, submission_verified_by: String, submission_vote_fish: String, submission_vote_not_fish: String)
case class targetDetails(target_name: String, target_id: String, timeStamp: String)
case class occurence(target_id: String, submission_year: String,count:String)
case class occurence_1(target_id: String, submission_month: String,count:String)

def main(args: Array[String]) = {

		val conf = new SparkConf().setAppName("socNet12").setMaster("local")
				val sc = new SparkContext(conf)
				val write = new PrintWriter(new File("countYear.txt"))
				val write1 = new PrintWriter(new File("count_op.txt"))
				write.write("target_id,submission_year,count")
				write1.write("target_id,submission_month,count")
				//	write.write("\n")

				val spark = SparkSession
				.builder()
				.appName("Spark SQL basic example")
				.config("spark.some.config.option", "some-value")
				.getOrCreate()

				val sqlContext = new org.apache.spark.sql.SQLContext(sc)
				import sqlContext.implicits._
				var count = new File("C:/Users/Parth/Desktop/Books/Sem3/Big_Data/Project/data/readFolder").listFiles().length
				for (i <- 1 to count) {

					val ip = sc.textFile("C:/Users/Parth/Desktop/Books/Sem3/Big_Data/Project/data/readFolder/" + i + ".csv")
							val header = ip.first()
							val input = ip.filter { x => x != header }

					//	write.write("\n")
					val test = input.map(x => x)
							println("testing input" + i)

							val fileSQL = input.filter(x=>x.split(",").length>10).map(_.split(",")).map(p =>data( p(0),p(1),p(2), p(3), p(4), p(5), p(6),p(7), p(8), p(9),p(10)))
							val df = fileSQL.toDF()
							val dfNew =  df.explode("submission_time","submissionNew"){submission_time:String => submission_time.split(" ")}
					val dfNewnew = df.map{
					case Row(phish_id: String, target_id: String, timestamp: String, submitter: String, submission_time: String, submission_url: String, submission_status: String, submission_verification_status: String, submission_verified_by: String, submission_vote_fish: String, submission_vote_not_fish: String) =>
					(phish_id,target_id,timestamp,submitter,submission_time.split(" ")(0),submission_time.split(" ")(1),submission_time.split(" ")(2),submission_url,submission_status,submission_verification_status, submission_vote_fish, submission_vote_not_fish)
					}.toDF("phish_id","target_id","timestamp","submitter","submission_month","submission_date","submission_year","submission_url","submission_status","submission_verification_status","submission_vote_fish","submission_vote_not_fish")
							//		dfNewnew.show()
							dfNewnew.createOrReplaceTempView("people1")
							//  fileSQL.take(4).foreach(println)

							// val query2 = spark.sql("SELECT target_id,count(distinct phish_id), submission_time from people where submission_verification_status='Verified: Is a phish' group by target_id, submission_time")
							// query2.show()

							val q3 = spark.sql("SELECT target_id,submission_year,count(distinct phish_id) from people1 where submission_verification_status='Verified: Is a phish' group by target_id, submission_year")
							val q4 = spark.sql("SELECT target_id,submission_month,count(distinct phish_id) from people1 where submission_verification_status='Verified: Is a phish' group by target_id, submission_month")
							//	q3.show()
							//	val p3 = q3.collect().mkString.length()
							val p2 = q3.collect().mkString("\n").replaceAll("\\[","").replaceAll("\\]","")
							val p3 = q4.collect().mkString("\n").replaceAll("\\[","").replaceAll("\\]","")


							//write.write(p2)
							write1.write("\n")
							write.write("\n")
							//  write.write(p3)
							write.write(p2)
							write1.write(p3)
							// write.write(p2.substring(1, p2.length() - 1))
				}

		write.close()
		write1.close()

		val targetFile = sc.textFile("C:/Users/Parth/Desktop/Books/Sem3/Big_Data/Project/data/new/target_id.csv")
		val targetHeader = targetFile.first()
		val targetData = targetFile.filter(x => x != targetHeader)

		val targetSQL = targetData.map(_.split(",")).map(p => targetDetails(p(0), p(1), p(2)))

		val df = targetSQL.toDF()   

		df.createOrReplaceTempView("target_table")

		val output=sc.textFile("countYear.txt")
		val outputHeader=output.first()
		val outputData=output.filter(x => x!= outputHeader)

		val outputSQL = outputData.map(_.split(",")).map(p => occurence(p(0), p(1), p(2)))

		val df1 = outputSQL.toDF()   

		df1.createOrReplaceTempView("output_table")
		val query1 = spark.sql("SELECT target_name,submission_year,count from target_table t inner join output_table o on t.target_id=o.target_id ")
		println("all data")
		query1.show()
println("query5")
		val output1=sc.textFile("count_op.txt")
		val output1Header=output1.first()
		val output1Data=output1.filter(x => x!= outputHeader)
		val output1SQL = output1Data.map(_.split(",")).map(p => occurence_1(p(0), p(1), p(2)))

		val df2 = output1SQL.toDF()   
		df2.createOrReplaceTempView("output1_table")
		
		val query5 = spark.sql("SELECT target_name,submission_month,count from target_table t inner join output1_table o on t.target_id=o.target_id ")
		query5.show()

		//      
		//      println("top 10 phisihig every year")
		//      val query2=spark.sql(" select target_name,submission_year from target_table t inner join output_table o on t.target_id=o.target_id group by submission_year order by submission_year desc limit 10")
		//      query2.show()


}

}