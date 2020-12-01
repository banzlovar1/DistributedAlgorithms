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
    spark.sparkContext.addFile("./data/airports.csv")
    spark.sparkContext.addFile("./data/airlines.csv")
    spark.sparkContext.addFile("./data/NewRouteAirports.csv")
    spark.sparkContext.addFile("./data/RouteAirports.csv")
    /**
     * Load CSV file into Spark SQL
     */
    // You can replace the path in .csv() to any local path or HDFS path
    var airports = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("airports.csv"))
    var airlines = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("airlines.csv"))
    var routeEdges = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("NewRouteAirports.csv"))
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


    /**
     * The GraphFrame function.
     */

     //Create a Vertex DataFrame with unique ID column "id"
    val v = routeAiports
    // Create an Edge DataFrame with "src" and "dst" columns
    val e = routeEdges
    // Create a GraphFrame
    val g = GraphFrame(v, e)


//    g.vertices.show()
//    g.edges.show()

//    val results = g.bfs.fromExpr("id='SEA'").toExpr("id='DEN'").maxPathLength(2).run()
//    results.show()

//    val result = g.bfs.fromExpr("id='SEA'").toExpr("id<>'SEA'").maxPathLength(2).run()
//    result.show(false)
//
//    val neigh = g.triplets.filter("src.id == 'SEA'")
//    neigh.show()

    /**
     * Sample of what the filter is supposed to do.
     * Pattern Matching given n hops
     */
    //    val pattern = "(x1) - [a] -> (x2); (x2) - [b] -> (x3)"
    //
    //    val paths = g.find(pattern).filter("x1.id == 'SEA' and x3.id != 'SEA' and x3.id == 'DEN' and b.Airline == a.Airline")
    //    paths.show()


    val alpha = "abcdefghijklmnopqrstuvwxyz"
    var pat = "(x1) - [a] -> (x2)"
    var max = 2
    var hops = 3
    for (a <- 2 to hops){
      pat = pat + "; (x" + a.toString + ") - [" + alpha.charAt(a-1) + "] -> " + "(x" + (a+1).toString + ")"
      max = max + 1
    }

    var src = "'SEA'"
    var dst = "'DEN'"
    var fill = "x1.id=="+src+" and x"+max.toString+".id=="+dst+" and x"+max.toString+".id!="+src
    for (a <- 2 to hops){
      fill = fill + " and " + alpha.charAt(a-2) + ".airline == "+alpha.charAt(a-1)+".airline"
    }
    println(pat)
    println(fill)

    val paths = g.find(pat).filter(fill)
    paths.show()

    /**
     * Stop the Spark session
     */
    spark.stop()
  }
}
