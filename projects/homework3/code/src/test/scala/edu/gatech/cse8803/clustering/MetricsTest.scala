package edu.gatech.cse8803.clustering

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import org.apache.spark.SparkContext
import org.scalatest.BeforeAndAfter
import org.apache.spark.SparkConf

class MetricsTest extends FlatSpec with BeforeAndAfter with Matchers {

  var sparkContext: SparkContext = _

  before {
    val config = new SparkConf().setAppName("Test Metrics").setMaster("local")
    sparkContext = new SparkContext(config)
  }

  after {
    sparkContext.stop()
  }

  def makeCases(c: Int, t: Int, count: Int): Seq[Tuple2[Int, Int]] = {
    var ret = scala.collection.mutable.ArrayBuffer[Tuple2[Int, Int]]()
    for (i <- 1 to count) {
      ret +:= (c, t)
    }
    return ret
  }

  "purity" should "give same answer as http://nlp.stanford.edu/IR-book/html/htmledition/evaluation-of-clustering-1.html" in {
    val allCases = Seq.concat(
        makeCases(1, 1, 0), // diamond
        makeCases(1, 2, 1),  // circle
        makeCases(1, 3, 5),  // cross
        makeCases(2, 1, 1),
        makeCases(2, 2, 4),
        makeCases(2, 3, 1),
        makeCases(3, 1, 3),
        makeCases(3, 2, 0),
        makeCases(3, 3, 2))
    val input = sparkContext.parallelize(allCases)
    Metrics.purity(input) should be ((5 + 4 + 3) / 17.0)
  }

  "purity" should "give same answer as http://stats.stackexchange.com/questions/95731/how-to-calculate-purity" in {
    val input = sparkContext.parallelize(Seq.concat(
        makeCases(1, 1, 0),  // diamond
        makeCases(1, 2, 53),  // circle
        makeCases(1, 3, 10),  // cross
        makeCases(2, 1, 0),
        makeCases(2, 2, 1),
        makeCases(2, 3, 60),
        makeCases(3, 1, 0),
        makeCases(3, 2, 16),
        makeCases(3, 3, 0)))
    Metrics.purity(input) should be ((53 + 60 + 16) / 140.0)
  }
}
