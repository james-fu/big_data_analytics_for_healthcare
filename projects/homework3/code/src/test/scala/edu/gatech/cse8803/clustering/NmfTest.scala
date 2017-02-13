package edu.gatech.cse8803.clustering

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import breeze.linalg.DenseMatrix

class NmfTest extends FlatSpec with Matchers with BeforeAndAfter {
  var sparkContext: SparkContext = _

  before {
    Logger.getRootLogger().setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.WARN)
    val config = new SparkConf().setAppName("Test NMF").setMaster("local")
    sparkContext = new SparkContext(config)
  }

  after {
    sparkContext.stop()
  }

  "getDenseMatrix" should "be in row-major" in {
    val input = new RowMatrix(sparkContext.parallelize(Seq(
        Vectors.dense(1, 2, 3),
        Vectors.dense(4, 5, 6))))
    val actual = NMF.getDenseMatrix(input)
    actual.cols should be (3)
    actual.rows should be (2)
    actual.valueAt(0, 0) should be (1)
    actual.valueAt(1, 1) should be (5)
    actual.valueAt(0, 2) should be (3)
  }

  "computeWTV" should "be correct" in {
    val W = new RowMatrix(sparkContext.parallelize(Seq(
        Vectors.dense(1, 4),
        Vectors.dense(2, 5),
        Vectors.dense(3, 6))))
    val V = new RowMatrix(sparkContext.parallelize(Seq(
        Vectors.dense(7, 8),
        Vectors.dense(9, 10),
        Vectors.dense(11, 12))))
    val actual = NMF.computeWTV(W, V)
    actual.rows should be (2)
    actual.cols should be (2)
    actual.valueAt(0, 0) should be (58)
    actual.valueAt(0, 1) should be (64)
    actual.valueAt(1, 0) should be (139)
    actual.valueAt(1, 1) should be (154)
  }

  "multiply" should "be correct" in {
    val X = new RowMatrix(sparkContext.parallelize(Seq(
        Vectors.dense(1, 2),
        Vectors.dense(3, 4),
        Vectors.dense(5, 6))))
    val d = new DenseMatrix(2, 3, Array(7.0, 8, 9, 10, 11, 12))
    val actual = NMF.multiply(X, d).rows.collect()
    actual(0)(0) should be (1*7 + 2*8)
    actual(0)(1) should be (1*9 + 2*10)
    actual(0)(2) should be (1*11 + 2*12)
    actual(1)(0) should be (3*7 + 4*8)
    actual(1)(1) should be (3*9 + 4*10)
    actual(1)(2) should be (3*11 + 4*12)
    actual(2)(0) should be (5*7 + 6*8)
    actual(2)(1) should be (5*9 + 6*10)
    actual(2)(2) should be (5*11 + 6*12)
  }

  "run" should "give two clusters" in {
    val input = new RowMatrix(sparkContext.parallelize(Seq(
        Vectors.dense(1, 0.1),
        Vectors.dense(1, 0.4),
        Vectors.dense(0.2, 1),
        Vectors.dense(0.3, 1))))
    val actual = NMF.run(input, 2, 50)
    val W = actual._1
    val H = actual._2
    val allW = W.rows.collect().map{v => v.toArray}
    //println(H)
    //allW.foreach{ v => println(v.mkString(" ")) }
    allW.length should be (input.numRows)
    allW.head.length should be (2)
    // Ensure that the first two rows, and the last two rows have the same sign.
    ((allW(0)(0) - allW(0)(1)) * (allW(1)(0) - allW(1)(1))) should be > 0.0
    ((allW(2)(0) - allW(2)(1)) * (allW(3)(0) - allW(3)(1))) should be > 0.0
  }
}
