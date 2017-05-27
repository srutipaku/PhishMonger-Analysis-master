
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import java.io._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import java.io.File

object TotalCount {
  case class file9(phish_id: String, target_id: String, timestamp: String, submitter: String, submission_time: String, submission_url: String, submission_status: String, submission_verification_status: String, submission_verified_by: String, submission_vote_fish: String, submission_vote_not_fish: String)
  case class targetDetails(target_name: String, target_id: String, timeStamp: String)
  case class occurence(target_id: String, occurence:String)

  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("socNet12").setMaster("local")
    val sc = new SparkContext(conf)
    val write = new PrintWriter(new File("outputQ2.txt"))
    val write1 = new PrintWriter(new File("FinalOP.txt"))
    write.write("target_id,occurence")
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

      //val year = readLine("YEAR:")
      //			val monnth = readLine("MONTH:")
      //val output = input.collect().foreach(println)

      //	val abc=input.map(x=>x.split('"')).collect().mkString
      //write.write(abc)
      write.write("\n")
      val test=input.map(x=> x)
      println("testing input"+i)
      
      
      val fileSQL = input.filter(x=>x.split(",").length>10).map(_.split(",")).map(p => file9(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10)))

      val df = fileSQL.toDF()   

      df.createOrReplaceTempView("people")
      val query1 = spark.sql("SELECT target_id,count(distinct phish_id) from people where submission_verification_status='Verified: Is a phish' group by target_id")
      //	val query = spark.sql("select phish_id,target_id,timestamp,submitter,submission_time,submission_url,submission_status,submission_vote_fish from people")
      val p2 = query1.collect().mkString
      
   //   val query2=spark.sql("SELECT target_id, ")
  
      write.write(p2.substring(1, p2.length() - 1))
    }
    
    write.close() 

    val targetFile= sc.textFile("C:/Users/Parth/Desktop/Books/Sem3/Big_Data/Project/data/new/target_id.csv")   
    val targetHeader=targetFile.first()
    val targetData=targetFile.filter(x => x!=targetHeader).map(line => line.split(",")).map(p =>(p(1), p(0)))
    
    println("Target key values id- name")
    targetData.foreach(println)
    
    val output=sc.textFile("outputQ2.txt")
    val outputHeader=output.first()
    val outputData=output.filter(x => x!= outputHeader).map(line => line.split(",")).map(p => (p(0),p(1)))
    
    println("output files id- occurence")
    outputData.foreach(println)
    
    val joined=targetData.join(outputData)
    
    println("Joined data")
    joined.foreach(println)
    
   
    
    println("EEE aayu")

    val newFi = sc.textFile("outputQ2.txt")
    val check = newFi.map(x => x)
    check.foreach(println)
    val manipulate = newFi.map { x => x.split(",") }.collect().mkString(",")

    //		val newtp = newFi.map { x =>x.split(",") }
    println("Have thase")
    //   	val op1 = newtp.take(3).foreach(println)
    write1.write(manipulate)

    write1.close()

  }
}

