/**
  * @author Hang Su <hangsu@gatech.edu>,
  * @author Sungtae An <stan84@gatech.edu>,
  */

package edu.gatech.cse8803.phenotyping

import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Date

object T2dmPhenotype {

  // criteria codes given
  val T1DM_DX = Set("250.01", "250.03", "250.11", "250.13", "250.21", "250.23",
    "250.31", "250.33", "250.41", "250.43", "250.51", "250.53", "250.61",
    "250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93")

  val T2DM_DX = Set("250.3", "250.32", "250.2", "250.22", "250.9", "250.92",
    "250.8", "250.82", "250.7", "250.72", "250.6", "250.62", "250.5", "250.52",
    "250.4", "250.42", "250.00", "250.02")

  val T1DM_MED = Set("lantus", "insulin glargine", "insulin aspart",
    "insulin detemir", "insulin lente", "insulin nph", "insulin reg",
    "insulin,ultralente")

  val T2DM_MED = Set("chlorpropamide", "diabinese", "diabanase", "diabinase",
    "glipizide", "glucotrol", "glucotrol xl", "glucatrol ", "glyburide",
    "micronase", "glynase", "diabetamide", "diabeta", "glimepiride", "amaryl",
    "repaglinide", "prandin", "nateglinide", "metformin", "rosiglitazone",
    "pioglitazone", "acarbose", "miglitol", "sitagliptin", "exenatide",
    "tolazamide", "acetohexamide", "troglitazone", "tolbutamide", "avandia",
    "actos", "actos", "glipizide")

  val ABNORMAL_LABS = Map( "HbA1c" -> 6.0, "Hemoglobin A1c" -> 6.0,
    "Fasting Glucose" -> 110, "Fasting blood glucose" -> 110,
    "fasting plasma glucose" -> 110, "Glucose" -> 110, "glucose" -> 110,
    "Glucose, Serum" -> 110)

  val DM_RELATED_DX = Set("790.21", "790.22", "790.2", "790.29", "648.81",
    "648.82", "648.83", "648.84", "648.0", "648.00", "648.01", "648.02",
    "648.03", "648.04", "791.5", "277.7", "V77.1", "256.4", "250.*")
  /**
    * Transform given data set to a RDD of patients and corresponding phenotype
    * @param medication medication RDD
    * @param labResult lab result RDD
    * @param diagnostic diagnostic code RDD
    * @return tuple in the format of (patient-ID, label). label = 1 if the patient is case, label = 2 if control, 3 otherwise
    */
  def transform(medication: RDD[Medication], labResult: RDD[LabResult], diagnostic: RDD[Diagnostic]): RDD[(String, Int)] = {
    /**
      * Remove the place holder and implement your code here.
      * Hard code the medication, lab, icd code etc. for phenotypes like example code below.
      * When testing your code, we expect your function to have no side effect,
      * i.e. do NOT read from file or write file
      *
      * You don't need to follow the example placeholder code below exactly, but do have the same return type.
      *
      * Hint: Consider case sensitivity when doing string comparisons.
      */
    /** Find CASE Patients */
    println("Finding CASE patients...")
    val casePatients = isCase(medication, diagnostic)
    println(casePatients.first())

    /** Find CONTROL Patients */
    println("Finding CONTROL patients...")
    val controlPatients = isControl(labResult, diagnostic)
    println(controlPatients.first())

    /** Find OTHER Patients */
    println("Finding OTHER patients...")
    // union case and control,
    val case_control_id = casePatients
      .union(controlPatients)
      .map( x => x._1 )

    // map over all arrays, saving patientID, (get all patientIDs)
    //val med_patients = medication.map( x => x.patientID).cache()
    val diag_patients = diagnostic.map( x => x.patientID).cache()
    //val lab_patients = labResult.map( x => x.patientID).cache()

    val all_patients = diag_patients.distinct()
      //.union(diag_patients)
      //.union(lab_patients)
      //.distinct()

    //med_patients.unpersist()
    diag_patients.unpersist()
    //lab_patients.unpersist()

    // difference the sets,
    val others = all_patients
      .subtract(case_control_id)
      .map( x => (x, 3))

    /** Once you find patients for each group, make them as a single RDD[(String, Int)] */
    val phenotypeLabel = casePatients.union(controlPatients).union(others)

    /** Return */
    phenotypeLabel
  }

  def isCase(medication: RDD[Medication], diagnostic: RDD[Diagnostic]): RDD[(String, Int)]= {
    val step1 = diagnostic
      .filter(y => !T1DM_DX.contains(y.code))
      .filter(y => T2DM_DX.contains(y.code))
      .map( x => (x.patientID, 1))
      .cache()

    val split1 = medication
      .filter(y => !T1DM_MED.contains(y.medicine.toLowerCase()))
      .map( x => (x.patientID, 1))

    val split2 = medication
      .filter(y => T1DM_MED.contains(y.medicine.toLowerCase()))

    val split3 = split2
      .filter(y => !T2DM_MED.contains(y.medicine.toLowerCase()))
      .map( x => (x.patientID, 1))

    def firstOccurrence(rdd: (String, Iterable[Medication])): Medication = {
      val by_date = rdd._2.map( x => (x.date, x)).toSeq

      val sorted = scala.util.Sorting.stableSort(by_date,
        (x:(Date, Medication), y:(Date, Medication)) => x._1.before(y._1))
      sorted.head._2
    }

    val first_t1 = medication
      .filter( y => T1DM_MED.contains(y.medicine.toLowerCase()))
      .map( y => (y.patientID, y))
      .groupByKey()
      .map( firstOccurrence )
      .map( y => (y.patientID, y))

    val first_t2 = medication
      .filter( y => T2DM_MED.contains(y.medicine.toLowerCase()))
      .map( y => (y.patientID, y))
      .groupByKey()
      .map( firstOccurrence )
      .map( y => (y.patientID, y))

    val joined_t = first_t1.join(first_t2)
      .filter( y => y._2._1.date.before(y._2._2.date))
      .map( y => (y._1, 1))


    //TODO: Last step
    val final1 = split1.intersection(step1, 16)
    val final2 = split3.intersection(step1, 16)
    val final3 = joined_t.intersection(step1, 16)
    val output = final1.union(final2).union(final3).distinct(16)

    step1.unpersist()
    split1.unpersist()
    first_t1.unpersist()
    first_t2.unpersist()


    //println(output.first())
    output
  }

  def isControl(labResult: RDD[LabResult], diagnostic: RDD[Diagnostic]): RDD[(String, Int)] = {

    def isNormalLab(row: LabResult): Boolean = {
      var out = true
      if (row.testName == "HbA1c" && row.value >= 6.0) {
        out = false
      }
      else if ( row.testName == "Fasting Glucose" && row.value >= 110) {
        out = false
      }
      else if ( row.testName == "Fasting blood glucose"  && row.value >= 110) {
        out = false
      }
      else if ( row.testName == "fasting plasma glucose" && row.value >= 110) {
        out = false
      }
      else if ( row.testName == "Glucose" && row.value > 110) {
        out = false
      }
      else if ( row.testName == "glucose" && row.value > 110) {
        out = false
      }
      else if ( row.testName == "Glucose, Serum" && row.value > 110) {
        out = false
      }

      out
    }

    val step1 = labResult
      .filter( x => x.testName.toLowerCase().contains("glucose") )
      .filter( isNormalLab )
      .map( x => (x.patientID, 2))

    val step2 = diagnostic
      .filter( x => !DM_RELATED_DX.map( y => x.code.matches(y))
                                  .reduce( (acc, z) => acc || z))
      .map( x => (x.patientID, 2))

    val output = step1.intersection(step2, 16).distinct(16)

    //println("isControl!!!")
    //println(output.first())

    output
  }
}
