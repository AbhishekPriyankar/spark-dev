package examples.streaming

import org.apache.spark.streaming.{ StreamingContext, Duration }
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

/**
 * Test code for the PairDStreamFunction.updateStateByKey(...).
 * Monitors the number of times a token appears over time.
 * Checkpointing is mandatory for cumulative stateful operations.
 */

object TestUpdateStateByKey {
	def main(args: Array[String]): Unit = {

		if (args.length != 3) {
			println("Usage: examples.streaming.TestUpdateStateByKey host port token_to_monitor")
			sys.exit(-1)
		}
		
		val ssc = new StreamingContext(new SparkConf().setAppName("TestUpdateStateByKeyJob"), Duration(1000))
		
		// enabling checkpoint dir, should be set to hdfs:// in production...
		ssc.checkpoint("file:/media/linux-1/spark-dev/checkpoint-tmp")
		
		/* As the application is a simple test we are overriding the default 
		Receiver's setting of StorageLevel.MEMORY_AND_DISK_SER_2 */
		val msg = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY)
		
		val tokenToMonitor = msg.flatMap(_.split(" "))
									.filter(_.equals(args(2)))
									.map(token => (token, 1))
		
		val occurrenceOverTime = tokenToMonitor.updateStateByKey(updateRunningTotal)
		
		println(">>> Computing the occurrence of a token over time.")
		
		occurrenceOverTime.print()
		
		ssc.start()
		ssc.awaitTermination()
		ssc.stop()
	}
	
	def updateRunningTotal(values: Seq[Int], state: Option[Int]): Option[Int] = {
		Some(values.size + state.getOrElse(0))
	}
}