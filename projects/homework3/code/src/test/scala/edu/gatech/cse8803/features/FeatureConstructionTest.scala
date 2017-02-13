package edu.gatech.cse8803.features

import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import edu.gatech.cse8803.model.Diagnostic
import edu.gatech.cse8803.model.Medication
import edu.gatech.cse8803.model.LabResult
import org.apache.spark.mllib.linalg.Vectors

class FeatureConstructionTest extends FlatSpec with BeforeAndAfter with Matchers {

  var sparkContext: SparkContext = _

  before {
    val config = new SparkConf().setAppName("Test FeatureConstruction").setMaster("local")
    sparkContext = new SparkContext(config)
  }

  after {
    sparkContext.stop()
  }

  "constructDiagnosticFeatureTuple" should "aggregate one event" in {
    val diags = sparkContext.parallelize(Seq(
        new Diagnostic("patient1", new Date(), "code1")));
    val actual = FeatureConstruction.constructDiagnosticFeatureTuple(diags).collect()
    val expected = Array(
        (("patient1", "code1"), 1.0))
    actual should be (expected)
  }

  "constructDiagnosticFeatureTuple" should "aggregate two different events" in {
    val diags = sparkContext.parallelize(Seq(
        new Diagnostic("patient1", new Date(), "code1"),
        new Diagnostic("patient1", new Date(), "code2")));
    val actual = FeatureConstruction.constructDiagnosticFeatureTuple(diags).collectAsMap()
    val expected = Map(
        (("patient1", "code1"), 1.0),
        (("patient1", "code2"), 1.0))
    actual should be (expected)
  }

  "constructDiagnosticFeatureTuple" should "aggregate two same events" in {
    val diags = sparkContext.parallelize(Seq(
        new Diagnostic("patient1", new Date(), "code1"),
        new Diagnostic("patient1", new Date(), "code1")));
    val actual = FeatureConstruction.constructDiagnosticFeatureTuple(diags).collect()
    val expected = Array(
        (("patient1", "code1"), 2.0))
    actual should be (expected)
  }

  "constructDiagnosticFeatureTuple" should "aggregate three events with duplication" in {
    val diags = sparkContext.parallelize(Seq(
        new Diagnostic("patient1", new Date(), "code1"),
        new Diagnostic("patient1", new Date(), "code1"),
        new Diagnostic("patient1", new Date(), "code2")));
    val actual = FeatureConstruction.constructDiagnosticFeatureTuple(diags).collectAsMap()
    val expected = Map(
        (("patient1", "code1"), 2.0),
        (("patient1", "code2"), 1.0))
    actual should be (expected)
  }

  "constructDiagnosticFeatureTuple" should "filter" in {
    val diags = sparkContext.parallelize(Seq(
        new Diagnostic("patient1", new Date(), "code1"),
        new Diagnostic("patient1", new Date(), "code1"),
        new Diagnostic("patient1", new Date(), "code2")));
    {
      val actual = FeatureConstruction.constructDiagnosticFeatureTuple(diags, Set("code2")).collect()
      actual should be (Array((("patient1", "code2"), 1.0)))
    }

    {
      val actual = FeatureConstruction.constructDiagnosticFeatureTuple(diags, Set("code1")).collect()
      actual should be (Array((("patient1", "code1"), 2.0)))
    }
  }

  /*=============================================*/

  "constructMedicationFeatureTuple" should "aggregate one event" in {
    val meds = sparkContext.parallelize(Seq(
        new Medication("patient1", new Date(), "code1")));
    val actual = FeatureConstruction.constructMedicationFeatureTuple(meds).collect()
    val expected = Array(
        (("patient1", "code1"), 1.0))
    actual should be (expected)
  }

   "constructMedicationFeatureTuple" should "aggregate two different events" in {
    val meds = sparkContext.parallelize(Seq(
        new Medication("patient1", new Date(), "code1"),
        new Medication("patient1", new Date(), "code2")));
    val actual = FeatureConstruction.constructMedicationFeatureTuple(meds).collectAsMap()
    val expected = Map(
        (("patient1", "code1"), 1.0),
        (("patient1", "code2"), 1.0))
    actual should be (expected)
  }

  "constructMedicationFeatureTuple" should "aggregate two same events" in {
    val meds = sparkContext.parallelize(Seq(
        new Medication("patient1", new Date(), "code1"),
        new Medication("patient1", new Date(), "code1")));
    val actual = FeatureConstruction.constructMedicationFeatureTuple(meds).collect()
    val expected = Array(
        (("patient1", "code1"), 2.0))
    actual should be (expected)
  }

  "constructMedicationFeatureTuple" should "aggregate three events with duplication" in {
    val meds = sparkContext.parallelize(Seq(
        new Medication("patient1", new Date(), "code1"),
        new Medication("patient1", new Date(), "code2"),
        new Medication("patient1", new Date(), "code2")));
    val actual = FeatureConstruction.constructMedicationFeatureTuple(meds).collectAsMap()
    val expected = Map(
        (("patient1", "code1"), 1.0),
        (("patient1", "code2"), 2.0))
    actual should be (expected)
  }

  "constructMedicationFeatureTuple" should "filter" in {
    val meds = sparkContext.parallelize(Seq(
        new Medication("patient1", new Date(), "code1"),
        new Medication("patient1", new Date(), "code2"),
        new Medication("patient1", new Date(), "code2")));
    {
      val actual = FeatureConstruction.constructMedicationFeatureTuple(meds, Set("code2")).collect()
      actual should be (Array((("patient1", "code2"), 2.0)))
    }

    {
      val actual = FeatureConstruction.constructMedicationFeatureTuple(meds, Set("code1")).collect()
      actual should be (Array((("patient1", "code1"), 1.0)))
    }
  }

  /*=============================================*/

  "constructLabFeatureTuple" should "aggregate one event" in {
    val labs = sparkContext.parallelize(Seq(
        new LabResult("patient1", new Date(), "code1", 42.0)));
    val actual = FeatureConstruction.constructLabFeatureTuple(labs).collect()
    val expected = Array(
        (("patient1", "code1"), 42.0))
    actual should be (expected)
  }

   "constructLabFeatureTuple" should "aggregate two different events" in {
    val labs = sparkContext.parallelize(Seq(
        new LabResult("patient1", new Date(), "code1", 42.0),
        new LabResult("patient1", new Date(), "code2", 24.0)));
    val actual = FeatureConstruction.constructLabFeatureTuple(labs).collectAsMap()
    val expected = Map(
        (("patient1", "code1"), 42.0),
        (("patient1", "code2"), 24.0))
    actual should be (expected)
  }

  "constructLabFeatureTuple" should "aggregate two same events" in {
    val labs = sparkContext.parallelize(Seq(
        new LabResult("patient1", new Date(), "code1", 42.0),
        new LabResult("patient1", new Date(), "code1", 24.0)));
    val actual = FeatureConstruction.constructLabFeatureTuple(labs).collect()
    val expected = Array(
        (("patient1", "code1"), 66.0 / 2))
    actual should be (expected)
  }

  "constructLabFeatureTuple" should "aggregate three events with duplication" in {
    val labs = sparkContext.parallelize(Seq(
        new LabResult("patient1", new Date(), "code1", 42.0),
        new LabResult("patient1", new Date(), "code1", 24.0),
        new LabResult("patient1", new Date(), "code2", 7475.0)));
    val actual = FeatureConstruction.constructLabFeatureTuple(labs).collectAsMap()
    val expected = Map(
        (("patient1", "code1"), 66.0 / 2),
        (("patient1", "code2"), 7475.0))
    actual should be (expected)
  }

  "constructLabFeatureTuple" should "filter" in {
    val labs = sparkContext.parallelize(Seq(
        new LabResult("patient1", new Date(), "code1", 42.0),
        new LabResult("patient1", new Date(), "code1", 24.0),
        new LabResult("patient1", new Date(), "code2", 7475.0)));
    {
      val actual = FeatureConstruction.constructLabFeatureTuple(labs, Set("code2")).collect()
      actual should be (Array((("patient1", "code2"), 7475.0)))
    }

    {
      val actual = FeatureConstruction.constructLabFeatureTuple(labs, Set("code1")).collect()
      actual should be (Array((("patient1", "code1"), 66.0 / 2)))
    }
  }

  /*=============================================*/

  "construct" should "give unique ID to codes" in {
    val patientFeatures = sparkContext.parallelize(Seq(
        (("patient1", "code2"), 42.0),
        (("patient1", "code1"), 24.0)))
    val actual = FeatureConstruction.construct(sparkContext, patientFeatures).collect()
    actual should be (Array(
        ("patient1", Vectors.dense(24.0, 42.0))))
  }

  "construct" should "give sparse vectors" in {
    val patientFeatures = sparkContext.parallelize(Seq(
        (("patient1", "code0"), 42.0),
        (("patient1", "code2"), 24.0),
        (("patient2", "code1"), 12.0)))
    val actual = FeatureConstruction.construct(sparkContext, patientFeatures).collectAsMap()
    actual should be (Map(
        ("patient1", Vectors.dense(42.0, 0.0, 24.0)),
        ("patient2", Vectors.dense(0.0, 12.0, 0.0))))
  }
}
