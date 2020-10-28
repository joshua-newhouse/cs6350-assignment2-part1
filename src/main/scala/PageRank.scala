package edu.utdallas.cs6350

import org.apache.spark.sql.SparkSession

object PageRank {
    def main(args: Array[String]): Unit = {
        if (args.length != 3) {
            println("Usage: PageRank InputFile Iterations OutputFile")
        }

        val sourceCSVFile = args(0)
        val iterations = args(1).toInt
        val outputFile = args(2)

        val spark = SparkSession.builder()
            .master("local[1]")
            .appName("Page Rank")
            .getOrCreate();

        val sc = spark.sparkContext

        val rawData = spark.read.option("header","true").option("inferSchema","true").csv(sourceCSVFile).rdd
        val airportConnections = rawData.map(row => (row(0),row(1))).groupByKey().map(x => (x._1, x._2.toList))

        val nAirports = airportConnections.count
        val initialRanks = 1.0 / nAirports

        var ranks = airportConnections.mapValues(v => initialRanks)

        for(_ <- 1 to iterations) {
            val nodeList = airportConnections.join(ranks)

            val contributions = nodeList.flatMap {
                case (_, (links, rank)) => links.map(dest => (dest, rank / links.size))
            }

            ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => .15 / nAirports + .85 * v)
        }

        val sortedRanks = ranks.sortBy(_._2, ascending = false).collect()
        val csv = sortedRanks.map(a => a._1.toString + "," + a._2.toString)
        csv.take(10)
        sc.parallelize(csv).coalesce(1,shuffle = true).saveAsTextFile(outputFile)
    }
}
