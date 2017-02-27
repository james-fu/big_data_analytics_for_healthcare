/**
 * @author Hang Su
 */
package edu.gatech.cse8803.features

import edu.gatech.cse8803.model.{LabResult, Medication, Diagnostic}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


object FeatureConstruction {

  /**
   * ((patient-id, feature-name), feature-value)
   */
  type FeatureTuple = ((String, String), Double)

  /**
   * Aggregate feature tuples from diagnostic with COUNT aggregation,
   * @param diagnostic RDD of diagnostic
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val diag_features = diagnostic
      .map( x => ((x.patientID, x.code), 1.0))
      .reduceByKey(_+_)

    diag_features
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation,
   * @param medication RDD of medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val med_features = medication
      .map( x => ((x.patientID, x.medicine), 1.0))
      .reduceByKey(_+_)

    med_features
  }

  /**
   * Aggregate feature tuples from lab result, using AVERAGE aggregation
   * @param labResult RDD of lab result
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val lab_features = labResult
      .map( x => ((x.patientID, x.testName), x.value))
      .aggregateByKey((0.0, 0.0))(
        (acc, x) => (acc._1 + x, acc._2 + 1.0),
        (acc, x) => (acc._1 + x._1, acc._2 + x._2))
      .mapValues( x => x._1 / x._2 )


    lab_features
  }

  /**
   * Aggregate feature tuple from diagnostics with COUNT aggregation, but use code that is
   * available in the given set only and drop all others.
   * @param diagnostic RDD of diagnostics
   * @param candiateCode set of candidate code, filter diagnostics based on this set
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic], candidateCode: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val diag_features = diagnostic
      .filter( x => candidateCode.contains(x.code))
      .map( x => ((x.patientID, x.code), 1.0))
      .reduceByKey(_+_)

    diag_features
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation, use medications from
   * given set only and drop all others.
   * @param medication RDD of diagnostics
   * @param candidateMedication set of candidate medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication], candidateMedication: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val med_features = medication
      .filter( x => candidateMedication.contains(x.medicine))
      .map( x => ((x.patientID, x.medicine), 1.0))
      .reduceByKey(_+_)

    med_features
  }


  /**
   * Aggregate feature tuples from lab result with AVERAGE aggregation, use lab from
   * given set of lab test names only and drop all others.
   * @param labResult RDD of lab result
   * @param candidateLab set of candidate lab test name
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult], candidateLab: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val lab_features = labResult
      .filter( x => candidateLab.contains(x.testName))
      .map( x => ((x.patientID, x.testName), x.value))
      .aggregateByKey((0.0, 0.0))(
        (acc, x) => (acc._1 + x, acc._2 + 1.0),
        (acc, x) => (acc._1 + x._1, acc._2 + x._2))
      .mapValues( x => x._1 / x._2 )


    lab_features
  }


  /**
   * Given a feature tuples RDD, construct features in vector
   * format for each patient. feature name should be mapped
   * to some index and convert to sparse feature format.
   * @param sc SparkContext to run
   * @param feature RDD of input feature tuples
   * @return
   */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {

    /** save for later usage */
    feature.cache()

    println("Building feature map...")
    /** create a feature name to id map*/
    val featureMap = feature
      .map( _._1._2 )
      .distinct
      .zipWithIndex
      .collect
      .toMap

    //println("FEATURE MAP!!")
    //println(feature_map.head)
    def vectorize(patient_features: (String, Iterable[(String, Double)])): (String, Vector) = {
      val mapped_features = patient_features._2
        .map( x => (featureMap(x._1).toInt, x._2))
        .toList

      val vec = Vectors.sparse(featureMap.size, mapped_features)

      (patient_features._1, vec)
    }
    /** transform input feature */
    val featureVectors = feature
      .map( x => (x._1._1, (x._1._2, x._2)))
      .groupByKey()
      // for each patient, for each reading, look up the index and put it in a sparse vector
      .map( vectorize )

    println(featureVectors.first())
    /**
     * Functions maybe helpful:
     *    collect
     *    groupByKey
     */

    /** The feature vectors returned can be sparse or dense. It is advisable to use sparse */
    featureVectors
  }
}


