/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import edu.gatech.cse8803.clustering.{NMF, Metrics}
import edu.gatech.cse8803.features.FeatureConstruction
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import edu.gatech.cse8803.phenotyping.T2dmPhenotype
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.{GaussianMixture, KMeans}
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Vectors, Vector}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


object Main {
  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //val sConf = new SparkConf()
    //val sc = new SparkContext(sConf)//createContext

    val sc = createContext
    sc.getConf.getAll.foreach(println)
    val sqlContext = new SQLContext(sc)

    /** initialize loading of data */
    val (medication, labResult, diagnostic) = loadRddRawData(sqlContext)
    val (candidateMedication, candidateLab, candidateDiagnostic) = loadLocalRawData

    /** conduct phenotyping */
    val phenotypeLabel = T2dmPhenotype.transform(medication, labResult, diagnostic)

    /** feature construction with all features */
    val featureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult),
      FeatureConstruction.constructMedicationFeatureTuple(medication)
    )

    val rawFeatures = FeatureConstruction.construct(sc, featureTuples)

    val (kMeansPurity, gaussianMixturePurity, nmfPurity) = testClustering(phenotypeLabel, rawFeatures)
    println(f"[All feature] purity of kMeans is: $kMeansPurity%.5f")
    println(f"[All feature] purity of GMM is: $gaussianMixturePurity%.5f")
    println(f"[All feature] purity of NMF is: $nmfPurity%.5f")

    /** feature construction with filtered features */
    val filteredFeatureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic, candidateDiagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult, candidateLab),
      FeatureConstruction.constructMedicationFeatureTuple(medication, candidateMedication)
    )

    val filteredRawFeatures = FeatureConstruction.construct(sc, filteredFeatureTuples)

    val (kMeansPurity2, gaussianMixturePurity2, nmfPurity2) = testClustering(phenotypeLabel, filteredRawFeatures)
    println(f"[Filtered feature] purity of kMeans is: $kMeansPurity2%.5f")
    println(f"[Filtered feature] purity of GMM is: $gaussianMixturePurity2%.5f")
    println(f"[Filtered feature] purity of NMF is: $nmfPurity2%.5f")
    sc.stop
  }

  def testClustering(phenotypeLabel: RDD[(String, Int)], rawFeatures:RDD[(String, Vector)]): (Double, Double, Double) = {
    import org.apache.spark.mllib.linalg.Matrix
    import org.apache.spark.mllib.linalg.distributed.RowMatrix

    /** scale features */
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(rawFeatures.map(_._2))
    val features = rawFeatures.map({ case (patientID, featureVector) => (patientID, scaler.transform(Vectors.dense(featureVector.toArray)))})
    val rawFeatureVectors = features.map(_._2).cache()

    /** reduce dimension */
    val mat: RowMatrix = new RowMatrix(rawFeatureVectors)
    val pc: Matrix = mat.computePrincipalComponents(10) // Principal components are stored in a local dense matrix.
    val featureVectors = mat.multiply(pc).rows

    val densePc = Matrices.dense(pc.numRows, pc.numCols, pc.toArray).asInstanceOf[DenseMatrix]
    /** transform a feature into its reduced dimension representation */
    def transform(feature: Vector): Vector = {
      Vectors.dense(Matrices.dense(1, feature.size, feature.toArray).multiply(densePc).toArray)
    }

    /** TODO: K Means Clustering using spark mllib
      *  Train a k means model using the variabe featureVectors as input
      *  Set maxIterations =20 and seed as 8803L
      *  Assign each feature vector to a cluster(predicted Class)
      *  Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
      *  Find Purity using that RDD as an input to Metrics.purity
      *  Remove the placeholder below after your implementation
      **/
    val kMeansPurity = 0.0

    /** TODO: GMMM Clustering using spark mllib
      *  Train a Gaussian Mixture model using the variabe featureVectors as input
      *  Set maxIterations =20 and seed as 8803L
      *  Assign each feature vector to a cluster(predicted Class)
      *  Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
      *  Find Purity using that RDD as an input to Metrics.purity
      *  Remove the placeholder below after your implementation
      **/
    val gaussianMixturePurity = 0.0


    /** NMF */
    val rawFeaturesNonnegative = rawFeatures.map({ case (patientID, f)=> Vectors.dense(f.toArray.map(v=>Math.abs(v)))})
    val (w, _) = NMF.run(new RowMatrix(rawFeaturesNonnegative), 3, 100)
    // for each row (patient) in W matrix, the index with the max value should be assigned as its cluster type
    val assignments = w.rows.map(_.toArray.zipWithIndex.maxBy(_._1)._2)
    // zip patientIDs with their corresponding cluster assignments
    // Note that map doesn't change the order of rows
    val assignmentsWithPatientIds=features.map({case (patientId,f)=>patientId}).zip(assignments)
    // join your cluster assignments and phenotypeLabel on the patientID and obtain a RDD[(Int,Int)]
    // which is a RDD of (clusterNumber, phenotypeLabel) pairs
    val nmfClusterAssignmentAndLabel = assignmentsWithPatientIds.join(phenotypeLabel).map({case (patientID,value)=>value})
    // Obtain purity value
    val nmfPurity = Metrics.purity(nmfClusterAssignmentAndLabel)

    (kMeansPurity, gaussianMixturePurity, nmfPurity)
  }

  /**
   * load the sets of string for filtering of medication
   * lab result and diagnostics
    *
    * @return
   */
  def loadLocalRawData: (Set[String], Set[String], Set[String]) = {
    val candidateMedication = Source.fromFile("data/med_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateLab = Source.fromFile("data/lab_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateDiagnostic = Source.fromFile("data/icd9_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    (candidateMedication, candidateLab, candidateDiagnostic)
  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[Medication], RDD[LabResult], RDD[Diagnostic]) = {
    /** You may need to use this date format. */
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

    /** load data using Spark SQL into three RDDs and return them
     * Hint: You can utilize edu.gatech.cse8803.ioutils.CSVUtils and SQLContext.
     *
     * Notes:Refer to model/models.scala for the shape of Medication, LabResult, Diagnostic data type.
     *       Be careful when you deal with String and numbers in String type.
     *       Ignore lab results with missing (empty or NaN) values when these are read in.
     *       For dates, use Date_Resulted for labResults and Order_Date for medication.
     * */

    var med_df = CSVUtils.loadCSVAsTable(sqlContext,
      "data/medication_orders_INPUT.csv", "Medication")

    med_df = med_df.select("Member_ID", "Order_Date", "Drug_Name")
    val medication: RDD[Medication] = med_df.map( x => Medication(x.getString(0),
      dateFormat.parse(x.getString(1)),
      x.getString(2))).sample(true, .01).repartition(5)


    var lab_df = CSVUtils.loadCSVAsTable(sqlContext,
      "data/lab_results_INPUT.csv", "LabResult")
    lab_df = lab_df.select("Member_ID", "Date_Resulted", "Test_Name", "Numeric_Result")
    lab_df = lab_df.na.drop()
    val labResult: RDD[LabResult] = lab_df.map( x => LabResult(x.getString(0),
      dateFormat.parse(x.getString(1)),
      x.getString(2),
      x.getDouble(3)))


    var diag_df = CSVUtils.loadCSVAsTable(sqlContext,
    "data/encounter_dx_INPUT.csv", "Diagnostic").sample(true, .01).cache()
    var event_df = CSVUtils.loadCSVAsTable(sqlContext,
    "data/encounter_INPUT.csv", "Diagnostic").sample(true, .01).cache()
    var full_df = event_df.join(diag_df)
    diag_df.unpersist()
    event_df.unpersist()

    diag_df = full_df.select("Member_ID", "Encounter_DateTime", "code")

    full_df.unpersist()
    val diagnostic: RDD[Diagnostic] = diag_df.map( x => Diagnostic(x.getString(0),
      dateFormat.parse(x.getString(1)),
      x.getString(2))).repartition(5)


    (medication, labResult, diagnostic)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(masterUrl)
      //.set("spark.driver.memory", "24g")
      //.set("spark.executor.memory", "6g")
      //.set("spark.driver.maxResultSize", "2g")
      //.set("spark.memory.storageFraction", "0.75")
      //.set("spark.default.parallelism", "30")
      .set("spark.local.dir", "/home/jeff/tmp")
      .set("spark.shuffle.file.buffer", "100m")
      .set("spark.cores.max", "32")

    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local[*]")

  def createContext: SparkContext = createContext("CSE 8803 Homework Two Application", "local[*]")
}
