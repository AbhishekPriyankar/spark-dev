package examples

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD

/**
 * Analyzes resources/purchases.log file.
 * Uses accumulators to detect various types of faulty records.
 */
object PurchaseLogAnalysis {
	def main(args: Array[String]): Unit = {

		val ctx = new SparkContext(new SparkConf().setAppName("PurchaseAnalysisJob"))

		val badPkts = ctx.accumulator(0, "Bad Packets")
		val zeroValueSales = ctx.accumulator(0, "Zero Value Sales")
		val missingFields = ctx.accumulator(0, "Missing Fields")
		val blankLines = ctx.accumulator(0, "Blank Lines")

		ctx.textFile("file:/media/linux-1/spark-dev/data/purchases.log", 4)
			.foreach { line =>

				if (line.length() == 0) blankLines += 1
				else if (line.contains("Bad data packet")) badPkts += 1
				else {
					val fields = line.split("\t")

					if (fields.length != 4) missingFields += 1
					else if (fields(3).toFloat == 0) zeroValueSales += 1
				}
			}

		println("Purchase Log Analysis Counters:")
		println(s"\tBad Data Packets=${badPkts.value}")
		println(s"\tZero Value Sales=${zeroValueSales.value}")
		println(s"\tMissing Fields=${missingFields.value}")
		println(s"\tBlank Lines=${blankLines.value}")
	}
}
