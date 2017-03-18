package edu.gatech.cse8803.randomwalk

import edu.gatech.cse8803.model.{PatientProperty, EdgeProperty, VertexProperty}
import org.apache.spark.graphx._

case class CountProperty(count: Integer, randVal: Double) extends VertexProperty

object RandomWalk {
  //private def makeMap(x: (VertexId, Int)*) = Map(x: _*)

  def randomWalkOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long, numIter: Int = 100, alpha: Double = 0.15): List[Long] = {
    /**
    Given a patient ID, compute the random walk probability w.r.t. to all other patients.
    Return a List of patient IDs ordered by the highest to the lowest similarity.
    For ties, random order is okay
    */

    // Can't do random walk in parallel bc we don't have access to the graph in
    // parallel.
    val sc = graph.vertices.sparkContext

    println(("PatientID", patientID))

    val newGraph = graph
      .mapVertices( (id, x) => if (id.toLong == patientID) (true, true, id.toLong, 0)
                         else if (x.isInstanceOf[PatientProperty]) (false, true, id.toLong, 0)
                         else (false, false, id.toLong, 0))
      .cache()



    def vProg(id: VertexId, value: (Boolean, Boolean, Long, Int) , message: Boolean): (Boolean, Boolean, Long, Int) = {
      if ((value._1 || message) && value._2) {(true, true, value._3, value._4 + 1)}
      else if ((value._1 || message)) {(true, value._2, value._3, value._4)}
      else {
        value
      }
    }

    def sendMsg(triplet: EdgeTriplet[(Boolean, Boolean, Long, Int), _]): Iterator[(VertexId, Boolean)] = {
      val src  = triplet.srcAttr
      val randVal = scala.util.Random.nextFloat
      val continue = randVal > alpha

      if (triplet.dstId == patientID) Iterator.empty
      else if (triplet.dstAttr._4 > 0) Iterator.empty
      else if (src._1 && continue) Iterator((triplet.dstId, true))
      else Iterator.empty
    }

    def mergeMsg(a: Boolean, b: Boolean): Boolean = {
      a || b
    }

    val output = newGraph.pregel(false, Int.MaxValue, EdgeDirection.Out)(
      vProg, sendMsg, mergeMsg)

    println("START!!!!")
    output.vertices.take(10).foreach(println)
    // Accumulate random walk results in to RDD, then do countByValue
    // Sort, then append patientIDs not included
    //println(mm)

    /** Remove this placeholder and implement your code */
    List(1,2,3,4,5)

    output
      .vertices
      .filter( x => x._1 != patientID )
      .filter( x => x._2._2 )
      .sortBy( x => x._2._4, false)
      .map( x => x._2._3)
      .collect()
      .toList
  }

  //def singleWalk(graph: Graph[VertexProperty, EdgeProperty], patientID: Long, alpha: Double): Unit ={
    //var visitedIds = Array()
    //var rand_alpha = randomNumber

    //var newId = patientID
    //while rand_alpha < alpha:
      //newId = step(graph, newId)

      //visitedIds.append(newID)
      //rand_alpha = randomNumber

    //visitedIDs
  //}
  //def step(graph: Graph[VertexProperty, EdgeProperty], currentID: Long): Long ={
    //graph.collectNeighborIds(j
      //k
    //val nextID
  //}
}
