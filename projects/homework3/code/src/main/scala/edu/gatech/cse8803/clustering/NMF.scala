package edu.gatech.cse8803.clustering

/**
  * @author Hang Su <hangsu@gatech.edu>
  */


import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, sum}
import breeze.linalg._
import breeze.numerics._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
//import org.apache.spark.mllib.util.fastSquaredDistance
import org.apache.spark.SparkContext

object NMF {

  /**
   * Run NMF clustering
   * @param V The original non-negative matrix
   * @param k The number of clusters to be formed, also the number of cols in W and number of rows in H
   * @param maxIterations The maximum number of iterations to perform
   * @param convergenceTol The maximum change in error at which convergence occurs.
   * @return two matrixes W and H in RowMatrix and DenseMatrix format respectively
   */
  def run(V: RowMatrix, k: Int, maxIterations: Int, convergenceTol: Double = 1e-4): (RowMatrix, BDM[Double]) = {
    println("NMF Run")
    val sc = V.rows.sparkContext

    //var W = getRowMatrix(BDM.rand[Double](V.numRows().toInt, k), k, sc, V.rows.getNumPartitions)

    val vects = V.rows
      .map( x => fromBreeze(BDV.rand[Double](k)))


    var W = new RowMatrix(vects)
    var H = BDM.rand[Double](k, V.numCols().toInt)

    var errorDiff = 10.0
    var error = getError(V, W, H)

    var iterCount = 0
    while ( iterCount < maxIterations && errorDiff > convergenceTol){

      val Hs = H*H.t
      W = dotDiv(dotProd(W, V.multiply(fromBreeze(H.t))), W.multiply(fromBreeze(Hs)))

      val Ws = toBreezeMatrix(W.computeGramianMatrix())

      H = H :* (computeWTV(W, V)+2.0e-15 :/ (Ws * H).toDenseMatrix + 2.0e-15)

      val newError = getError(V, W, H)

      errorDiff = abs(newError - error)

      error = newError

      W.rows.cache()
      V.rows.cache()

      //println("ERROR!!!!!!!!!!!!!!!!!!!")
      //println(errorDiff)
      //println(iterCount)

      iterCount = iterCount + 1
    }



    (W, H)
  }


  /**
  * RECOMMENDED: Implement the helper functions if you needed
  * Below are recommended helper functions for matrix manipulation
  * For the implementation of the first three helper functions (with a null return),
  * you can refer to dotProd and dotDiv whose implementation are provided
  */
  /**
  * Note:You can find some helper functions to convert vectors and matrices
  * from breeze library to mllib library and vice versa in package.scala
  */

  /** compute the mutiplication of a RowMatrix and a dense matrix */
  def multiply(X: RowMatrix, d: BDM[Double]): RowMatrix = {
    val byRow = X.multiply(fromBreeze(d))

    byRow
  }

  def transpose(X: RowMatrix): RowMatrix = {
    getRowMatrix(getDenseMatrix(X).t, X.numRows().toInt, X.rows.sparkContext, X.rows.getNumPartitions)
  }


  def getError(V: RowMatrix, W: RowMatrix, H: BDM[Double]): Double = {
    //println("W shape")
    //println (W.numRows())
    //println (W.numCols())

    //println("V shape")
    //println (V.numRows())
    //println (V.numCols())

    //println("H shape")
    //println (H.rows)
    //println (H.cols)


    val WH = multiply(W, H)

    //println("WH shape")
    //println (WH.numRows())
    //println (WH.numCols())

    //println(WH.rows.count())
    //println("MULTIPLIED!!!!!")
    val diff = getDenseMatrix(V)-getDenseMatrix(WH)
    var error = sqrt(sum(diff :* diff))

    error
  }

  def getRowMatrix(X: BDM[Double], numCols: Int, sc: SparkContext, numPartitions: Int): RowMatrix = {
      val output = X
        .t
        .toDenseVector
        .toArray
        .grouped(numCols)
        .toList
        .map( Vectors.dense )

      val byRow = sc.parallelize(output).repartition(numPartitions)

      new RowMatrix(byRow)
  }

 /** get the dense matrix representation for a RowMatrix */
  def getDenseMatrix(X: RowMatrix): BDM[Double] = {
    val data = X.rows.map( _.toArray ).collect().flatten
    val rows = X.numRows()
    val cols = X.numCols()

    val denseMat = new BDM(cols.toInt, rows.toInt, data).t
    denseMat
  }

  /** matrix multiplication of W.t and V */
  def computeWTV(W: RowMatrix, V: RowMatrix): BDM[Double] = {
    val denseW = getDenseMatrix(W)
    val denseV = getDenseMatrix(V)

    denseW.t * denseV
  }

  /** dot product of two RowMatrixes */
  def dotProd(X: RowMatrix, Y: RowMatrix): RowMatrix = {
      //println("dotProd")
      //println("X partitions")
      //println(X.rows.getNumPartitions)
      //println(X.numRows())
      //println(X.numCols())
      //println("Y partitions")
      //println(Y.rows.getNumPartitions)
      //println(Y.numRows())
      //println(Y.numCols())
    //val xRows = X.rows
      //.zipWithIndex
      //.map( x => (x._2, x._1))

    //val yRows = Y.rows
      //.zipWithIndex
      //.map( x => (x._2, x._1))

    //val zippedRows = xRows.join(yRows).map(_._2)
    //val rows = zippedRows.map{case (v1: Vector, v2: Vector) =>
      //(toBreezeVector(v1) :* toBreezeVector(v2))
    //}.map(fromBreeze)

    val rows = X.rows.zip(Y.rows).map{case (v1: Vector, v2: Vector) =>
      toBreezeVector(v1) :* toBreezeVector(v2)
    }.map(fromBreeze)
    new RowMatrix(rows)
  }

  /** dot division of two RowMatrixes */
  def dotDiv(X: RowMatrix, Y: RowMatrix): RowMatrix = {
      //println("dotDiv")
      //println("X partitions")
      //println(X.rows.getNumPartitions)
      //println(X.numRows())
      //println(X.numCols())
      //println("Y partitions")
      //println(Y.rows.getNumPartitions)
      //println(Y.numRows())
      //println(Y.numCols())
    //val xRows = X.rows
      //.zipWithIndex
      //.map( x => (x._2, x._1))

    //val yRows = Y.rows
      //.zipWithIndex
      //.map( x => (x._2, x._1))

    //val zippedRows = xRows.join(yRows).map(_._2)

    //val rows = zippedRows.map{case (v1: Vector, v2: Vector) =>
      //toBreezeVector(v1)+2.0e-15 :/ toBreezeVector(v2).mapValues(_ + 2.0e-15)
    //}.map(fromBreeze)

    val rows = X.rows.zip(Y.rows).map{case (v1: Vector, v2: Vector) =>
      toBreezeVector(v1) :/ toBreezeVector(v2).mapValues(_ + 2.0e-15)
    }.map(fromBreeze)
    new RowMatrix(rows)
  }
}
