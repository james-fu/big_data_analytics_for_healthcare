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
    val sc = V.rows.sparkContext
    /**
     * TODO 1: Implement your code here
     * Initialize W, H randomly
     * Calculate the initial error (Euclidean distance between V and W * H)
     */
    var W = getRowMatrix(BDM.rand[Double](V.numRows().toInt, k), V.numCols().toInt, sc)
    var H = BDM.rand[Double](k, V.numCols().toInt)
    var errorDiff = getError(V, W, H)
    var error = getError(V, W, H)

    var iterCount = 0

    /**
     * TODO 2: Implement your code here
     * Iteratively update W, H in a parallel fashion until error falls below the tolerance value
     * The updating equations are,
     * H = H.* W^T^V ./ (W^T^W H)
     * W = W.* VH^T^ ./ (W H H^T^)
     */
    //var Hs = Matrix
    //var newH = BDM
    //var Ws = Matrix
    //var newW = RowMatrix

    while ( iterCount < maxIterations && errorDiff > convergenceTol){

      val Hs = H*H.t

      val newW = dotDiv(dotProd(W, V.multiply(fromBreeze(H.t))), W.multiply(fromBreeze(Hs)))

      val Ws = toBreezeMatrix(W.computeGramianMatrix())

      val newH = H :* (computeWTV(W, V) :/ (Ws * H).toDenseMatrix)


      val newError = getError(V, newW, newH)

      errorDiff = abs(newError - error)

      error = newError
      H = newH
      W = newW

      W.rows.cache()
      V.rows.cache()

      println("ERROR!!!!!!!!!!!!!!!!!!!")
      println(errorDiff)
      //println(error)
      println(iterCount)

      iterCount = iterCount + 1
    }



    /** TODO: Remove the placeholder for return and replace with correct values */
    (W, H)
    //(new RowMatrix(V.rows.map(_ => BDV.rand[Double](k)).map(fromBreeze).cache), BDM.rand[Double](k, V.numCols().toInt))
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
    getRowMatrix(getDenseMatrix(X).t, X.numRows().toInt, X.rows.sparkContext)
  }


  def getError(V: RowMatrix, W: RowMatrix, H: BDM[Double]): Double = {
    val WH = multiply(W, H)
    val diff = getDenseMatrix(V)-getDenseMatrix(WH)
    var error = sqrt(sum(diff :* diff))

    error
  }

  def getRowMatrix(X: BDM[Double], numRows: Int, sc: SparkContext): RowMatrix = {
      val output = X
        .t
        .toDenseVector
        .toArray
        .grouped(numRows)
        .toList
        .map( Vectors.dense )

      val byRow = sc.parallelize(output)

      new RowMatrix(byRow)
  }

 /** get the dense matrix representation for a RowMatrix */
  def getDenseMatrix(X: RowMatrix): BDM[Double] = {
    val data = X.rows.map( _.toArray ).collect().flatten
    val rows = X.numRows()
    val cols = X.numCols()
    //println("Rows:")
    //println(rows)
    //println("Cols:")
    //println(cols)

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
    val rows = X.rows.zip(Y.rows).map{case (v1: Vector, v2: Vector) =>
      toBreezeVector(v1) :* toBreezeVector(v2)
    }.map(fromBreeze)
    new RowMatrix(rows)
  }

  /** dot division of two RowMatrixes */
  def dotDiv(X: RowMatrix, Y: RowMatrix): RowMatrix = {
    val rows = X.rows.zip(Y.rows).map{case (v1: Vector, v2: Vector) =>
      toBreezeVector(v1) :/ toBreezeVector(v2).mapValues(_ + 2.0e-15)
    }.map(fromBreeze)
    new RowMatrix(rows)
  }
}
