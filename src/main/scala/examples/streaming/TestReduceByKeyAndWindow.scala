package examples.streaming

import org.apache.spark.streaming.{ StreamingContext, Duration }
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

/**
 * Test code for the PairDStreamFunction.reduceByKeyAndWindow(...).
 * Monitors the number of times a token appears in a window.
 * Checkpointing is mandatory for cumulative stateful operations.
 * By default persistence is enabled for stateful operations.
 */

object TestReduceByKeyAndWindow {
	def main(args: Array[String]): Unit = {
		if (args.length != 3) {
			println("Usage: examples.streaming.TestReduceByKeyAndWindow host port token_to_monitor")
			sys.exit(-1)
		}

		val ssc = new StreamingContext(new SparkConf().setAppName("TestReduceByKeyAndWindowJob"), Duration(1000))

		// enabling checkpoint dir, should be set to hdfs:// in production...
		ssc.checkpoint("file:/media/linux-1/spark-dev/checkpoint-tmp")

		/* As the application is a simple test we are overriding the default 
		Receiver's setting of StorageLevel.MEMORY_AND_DISK_SER_2 */
		val receiver = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY)

		val tokenToMonitor = receiver.flatMap(_.split(" "))
			.filter(_.equals(args(2)))
			.map(token => (token, 1))

		val reducedOverWindow = tokenToMonitor
			// calculate the occurrence of the token over the 
			// last 3s and compute the results every 2s
			.reduceByKeyAndWindow(_ + _, // adding elements in the new batches entering the window 
				_ - _, // removing elements from the new batches exiting the window 
				Duration(3000), Duration(2000))
		
		reducedOverWindow.checkpoint(Duration(30000)) // 30 seconds checkpoint

		println(">>> Computing the occurrence of a token over time.")

		reducedOverWindow.print()

		ssc.start()
		ssc.awaitTermination()
		ssc.stop()
	}
}