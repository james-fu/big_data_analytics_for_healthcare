/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.graphconstruct

import edu.gatech.cse8803.model._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object GraphLoader {
  /** Generate Bipartite Graph using RDDs
    *
    * @input: RDDs for Patient, LabResult, Medication, and Diagnostic
    * @return: Constructed Graph
    *
    * */
  def load(patients: RDD[PatientProperty], labResults: RDD[LabResult],
           medications: RDD[Medication], diagnostics: RDD[Diagnostic]): Graph[VertexProperty, EdgeProperty] = {

    val sc = patients.sparkContext
    /** HINT: See Example of Making Patient Vertices Below */
    val vertexPatient: RDD[(VertexId, VertexProperty)] = patients
      .map(patient => (patient.patientID.toLong, patient.asInstanceOf[VertexProperty]))
      .cache()
    val patientCount = vertexPatient.count() + 1

    /////////////////////////////////////////////////////////////////////////
    val vertexLabRDD = labResults
      .map(_.labName)
      .distinct()
      .zipWithIndex()
      .map( withInd => (withInd._1, withInd._2 + patientCount))

    val vertexLabResult: RDD[(VertexId, VertexProperty)] = vertexLabRDD
      .map( labWithInd => (labWithInd._2,
        LabResultProperty(labWithInd._1).asInstanceOf[VertexProperty]))
    val labCount = vertexLabResult.count() + patientCount + 1
    ////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////
    val vertexMedicationRDD = medications
      .map(_.medicine)
      .distinct()
      .zipWithIndex()
      .map( withInd => (withInd._1, withInd._2 + labCount))

    val vertexMedication: RDD[(VertexId, VertexProperty)] = vertexMedicationRDD
      .map(codeWithInd => (codeWithInd._2,
        MedicationProperty(codeWithInd._1).asInstanceOf[VertexProperty]))
    val medCount = vertexMedication.count() + patientCount + labCount + 1
    ///////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////
    val vertexDiagnosticRDD = diagnostics
      .map(_.icd9code)
      .distinct()
      .zipWithIndex()
      .map( withInd => ( withInd._1, withInd._2 + medCount))

    val vertexDiagnostic: RDD[(VertexId, VertexProperty)] = vertexDiagnosticRDD
      .map(codeWithInd => (codeWithInd._2,
        DiagnosticProperty(codeWithInd._1).asInstanceOf[VertexProperty]))

    ///////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////
    //val patientMapBC = sc.broadcast(vertexPatientRDD.collect.toMap)
    val labMapBC = sc.broadcast(vertexLabRDD.collect.toMap)
    val medMapBC = sc.broadcast(vertexMedicationRDD.collect.toMap)
    val diagMapBC = sc.broadcast(vertexDiagnosticRDD.collect.toMap)


    val edgePatientLab: RDD[Edge[EdgeProperty]] = labResults
      .map( lab => ((lab.patientID, lab.labName), lab))
      .groupByKey()
      .map( patLab => ((patLab._1._1, patLab._1._2), patLab._2.last))
      .flatMap( patLab => List(Edge(patLab._1._1.toLong,
                                    labMapBC.value(patLab._1._2),
                                    PatientLabEdgeProperty(patLab._2)),
                               Edge(labMapBC.value(patLab._1._2),
                                    patLab._1._1.toLong,
                                    PatientLabEdgeProperty(patLab._2))))

    val edgePatientMedication: RDD[Edge[EdgeProperty]] = medications
      .map( med => ((med.patientID, med.medicine), med))
      .groupByKey()
      .map( patLab => ((patLab._1._1, patLab._1._2), patLab._2.last))
      .flatMap( patLab => List(Edge(patLab._1._1.toLong,
                                    medMapBC.value(patLab._1._2),
                                    PatientMedicationEdgeProperty(patLab._2)),
                               Edge(medMapBC.value(patLab._1._2),
                                    patLab._1._1.toLong,
                                    PatientMedicationEdgeProperty(patLab._2))))

    val edgePatientDiagnostic: RDD[Edge[EdgeProperty]] = diagnostics
      .map( diag => ((diag.patientID, diag.icd9code), diag))
      .groupByKey()
      .map( patLab => ((patLab._1._1, patLab._1._2), patLab._2.last))
      .flatMap( patLab => List(Edge(patLab._1._1.toLong,
                                    diagMapBC.value(patLab._1._2),
                                    PatientDiagnosticEdgeProperty(patLab._2)),
                               Edge(diagMapBC.value(patLab._1._2),
                                    patLab._1._1.toLong,
                                    PatientDiagnosticEdgeProperty(patLab._2))))

    val verteces = vertexPatient
      .union(vertexLabResult)
      .union(vertexMedication)
      .union(vertexDiagnostic)

    val edges = edgePatientLab
      .union(edgePatientMedication)
      .union(edgePatientDiagnostic)

    // Making Graph
    val graph: Graph[VertexProperty, EdgeProperty] = Graph(verteces, edges)

    graph
  }
}
