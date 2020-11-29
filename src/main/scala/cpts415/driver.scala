package cpts415

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkFiles
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.graphframes.GraphFrame

object driver {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def main(args: Array[String]) {
    /**
     * Start the Spark session
     */
    val spark = SparkSession
      .builder()
      .appName("cpts415-bigdata")
      .config("spark.some.config.option", "some-value")//.master("local[*]")
      .getOrCreate()
    spark.sparkContext.addFile("/home/ubuntu/CPTS-415-Project-Template/data/airports.csv")
    spark.sparkContext.addFile("/home/ubuntu/CPTS-415-Project-Template/data/airlines.csv")
    spark.sparkContext.addFile("/home/ubuntu/CPTS-415-Project-Template/data/RouteRelationships.csv")
    spark.sparkContext.addFile("/home/ubuntu/CPTS-415-Project-Template/data/RouteAirports.csv")
    /**
     * Load CSV file into Spark SQL
     */
    // You can replace the path in .csv() to any local path or HDFS path
    var airports = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("airports.csv"))
    var airlines = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("airlines.csv"))
    var routeEdges = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("RouteRelationships.csv"))
    var routeAiports = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("RouteAirports.csv"))
    airports.createOrReplaceTempView("airports")
    airlines.createOrReplaceTempView("airlines")
    
    /**
     * The basic Spark SQL functions
     */
    println("Find the names of Airports in a given Country")
    var new_users = spark.sql(
      """
        |SELECT Name
        |FROM airports
        |WHERE airports.Country = 'United States'
        |""".stripMargin)
    new_users.show()

    /**
     * Top 5 Countries
     */
    println("Top Countries by Airport")
    var freq = spark.sql(
      """
        |SELECT Country, COUNT(*)
        |FROM airports
        |GROUP BY airports.Country
        |ORDER BY 2 DESC
        |LIMIT 5
        |""".stripMargin
    )
    freq.show()

    println("Active Airlines in US")
    var CountryAirlines = spark.sql(
      """
        |SELECT Name
        |FROM airlines
        |WHERE airlines.Country = 'United States' AND airlines.Active = 'Y'
        |""".stripMargin
    )
    CountryAirlines.show()
    new_users.write.mode(SaveMode.Overwrite).options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("data/new-users.csv")

    /**
     * The GraphFrame function.
     */

    // Create a Vertex DataFrame with unique ID column "id"
    val v = routeAiports
    // Create an Edge DataFrame with "src" and "dst" columns
    val e = routeEdges
    // Create a GraphFrame
    val g = GraphFrame(v, e)


//    g.vertices.show()
    g.edges.show()

//    val results = g.bfs.fromExpr("id='SEA'").toExpr("id='DEN'").maxPathLength(2).run()
//    results.show()

    val result = g.bfs.fromExpr("id='SEA'").toExpr("id<>'SEA'").maxPathLength(2).run()
    result.show(false)
    
    /**
     * DO NOT RECOMMEND. Shortest path using GraphX (only show distance): https://spark.apache.org/docs/latest/graphx-programming-guide.html#pregel-api
     */

    // A graph with edge attributes containing distances
    val graph: Graph[Long, Double] =
    GraphGenerators.logNormalGraph(spark.sparkContext, numVertices = 100).mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId = 42 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))

    /**
     * Stop the Spark session
     */
    spark.stop()
  }
}
