//script to test Spark Cassandra functionality
//for Spark scala-shell
 
 
//#!sudo bin/cassandra
//#!sudo bin/spark-shell --jars /home/project/spark-cassandra-connector-java-assembly-1.3.0-SNAPSHOT.jar
 
 
 
//stop old context
sc.stop
 
//import library for configuration
import org.apache.spark.SparkConf
 
//create new configuration
val conf = (new SparkConf(true)
			.set("spark.cassandra.connection.host", "localhost"))
 
//import context library
import org.apache.spark.SparkContext
 
//create new context
val sc = new SparkContext(conf)
 
 
 
//create scala class for trainHistory
case class trainHistory(
	id: Double,
	chain: Double,
	offer: Double,
	market: Double,
	repeattrips: Double
	)
 
//load csv data
val data = sc.textFile("/home/project/Documents/trainHistory.csv")
 
//parse data to class
val parsed = (data.filter(line => !line.contains("id"))
	.map {row =>
    val splitted = row.split(",")
    val Array(id,chain, offer, market, repeattrips) = 
    splitted.slice(0,5).map(_.toDouble)
    trainHistory (id, chain, offer, market, repeattrips)
})
 
//check parsing results
parsed.take(2).foreach(println)
parsed.count
 
//import Cassandra connectors
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
 
 
//clean keyspace
CassandraConnector(sc.getConf).withSessionDo { session =>
	session.execute("DROP KEYSPACE IF EXISTS promo")}
 
 
//create keyspace, table
CassandraConnector(sc.getConf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS promo WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute ("""CREATE TABLE IF NOT EXISTS
        promo.train (
            id double primary key,
            chain double,
            offer double,
        	market double,
        	repeattrips double
        )
    """)
}
 
//save data to Cassandra
parsed.saveToCassandra ("promo", "train")
 
//create class train
case class train(
	id: Double,
	chain: Double,
	offer: Double,
	market: Double,
	repeattrips: Double
	)
 
//load trainHistory from cassandra
val rdd = sc.cassandraTable[train]("promo", "train").cache()
 
//check loaded data
rdd.count
rdd.first
 
//import RDD and Vectors libraries
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
 
 
//create vector of repeattrips observations
val observations1: RDD[Vector] = 
	rdd.map(s => Vectors.dense
	(s.repeattrips))
 
//import statistics library
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
 
 
//calculate statistics for repeattrips
val summary: MultivariateStatisticalSummary = Statistics.colStats(observations1)
 
 
//print out statistics
println(summary.count)
println(summary.max)
println(summary.mean)
println(summary.min)
println(summary.variance)
 
 
//print out 
scala.tools.nsc.io.File("output.txt").writeAll("\n" + "repeattrips.count = " 
+ summary.count + "\n")
 
scala.tools.nsc.io.File("output.txt").appendAll("\n" + "repeattrips.max = " 
+ summary.max + "\n")
 
scala.tools.nsc.io.File("output.txt").appendAll("\n" + "repeattrips.mean = " 
+ summary.mean + "\n")
 
scala.tools.nsc.io.File("output.txt").appendAll("\n" + "repeattrips.min = " 
+ summary.min + "\n")
 
scala.tools.nsc.io.File("output.txt").appendAll("\n" + "repeattrips.variance = " 
+ summary.variance + "\n")
 
 
//create class for offers
case class offers(
	offer: Double,
	category: Double,
	quantity: Double,
	company: Double,
	offervalue: Double,
	brand: Double
	)
 
//load offers.csv
val offersdata = sc.textFile("/home/project/Documents/offers.csv")
 
//parse offers data
val parsedof = (offersdata.filter(line => !line.contains("offer"))
	.map {row =>
    val splitted = row.split(",")
    val Array(offer,category, quantity, company, offervalue,
    brand) = 
    splitted.slice(0,6).map(_.toDouble)
    offers (offer,category, quantity, company, offervalue, brand)
})
 
//check the parsed data
parsedof.take(2).foreach(println)
parsedof.count
 
 
//create offers table
CassandraConnector(sc.getConf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS promo WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute ("""CREATE TABLE IF NOT EXISTS
        promo.offers (
            offer double primary key,
            category double,
            quantity double,
        	company double,
        	offervalue double,
        	brand double
        )
    """)
}
 
 
//save offers data to Cassandra
parsedof.saveToCassandra ("promo", "offers")
 
 
//load offers data from Cassandra
val rddoffer = sc.cassandraTable[offers]("promo", "offers").cache()
 
 
//check loaded data
rddoffer.count
rddoffer.first
 
//join data
val internalJoin = (sc.cassandraTable[train]("promo","train").joinWithCassandraTable[offers]("promo","offers")
.on(SomeColumns("offer")))
 
//check joined
internalJoin.count
internalJoin.first
 
//import libraries
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
 
//prepare data
val seriesRT: RDD[Double] = internalJoin.map{case(k,v) => (k.repeattrips)}
val seriesOV: RDD[Double] = internalJoin.map{case(k,v) => (v.offervalue)}
 
//calculate coreelation
val correlationRTOV: Double = Statistics.corr(seriesRT, seriesOV, "pearson")
 
 
//save to output file
scala.tools.nsc.io.File("output.txt").appendAll("\n" + "correlation RT and OV = " 
+ correlationRTOV + "\n")
 
 
//import library
import org.apache.spark.mllib.clustering.KMeans
 
//prepare data
val trainingData: RDD[Vector] = 
	rdd.map(s => Vectors.dense
	(s.chain, s.offer, s.market, s.repeattrips))
	
//set params
val numClusters = 20
val numIterations = 20
 
//clustering
val clusters = KMeans.train(trainingData, numClusters, numIterations)	
	
//cluster centers	
clusters.clusterCenters
 
//save to output file
val save = sc.parallelize(clusters.clusterCenters)
save.saveAsTextFile("clusters")
 
//exit
exit
