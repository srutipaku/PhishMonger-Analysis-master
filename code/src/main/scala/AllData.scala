import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import java.io._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import java.io.File

object AllData {
case class data(phish_id: String, target_id: String, timestamp: String, submitter: String, submission_time: String, submission_url: String, submission_status: String, submission_verification_status: String, submission_verified_by: String, submission_vote_fish: String, submission_vote_not_fish: String)

def main(args: Array[String]) = {
		println("Hello")
		val conf = new SparkConf().setAppName("AllData").setMaster("local")
		val sc = new SparkContext(conf)
		val write = new PrintWriter(new File("AllData.csv"))
		write.write("phish_id,target_id,timestamp,submitter,submission_month,submission_date,submission_year,submission_url,submission_status,submission_verification_status,submission_vote_fish,submission_vote_not_fish")
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
							dfNewnew.createOrReplaceTempView("people_alldata")
    			val q1 = spark.sql("SELECT * from people_alldata")
    			val p1 = q1.collect().mkString("\n").replaceAll("\\[","").replaceAll("\\]","")
    		
    			
    			write.write("\n")
    			write.write(p1)
		}
		write.close()

}
}