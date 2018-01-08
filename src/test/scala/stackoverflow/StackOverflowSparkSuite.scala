package stackoverflow

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}


@RunWith(classOf[JUnitRunner])
class StackOverflowSparkSuite extends FunSuite with Matchers with SparkContextSetup {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sparkContext : SparkContext = new SparkContext(conf)


  withSparkContext { (sparkContext: SparkContext) =>

    val input = sparkContext.textFile("src/main/resources/stackoverflow/stackoverflow.csv")

    val data:RDD[Posting] = StackOverflow.rawPostings(input)

    //val data: Seq[Posting] = Seq()

    val grouped = StackOverflow.groupedPostings(data)

    val scored = StackOverflow.scoredPostings(grouped)

    /*
    Test 1:
    TODO: place custom aggregator of  for checking RDD in test (via 'contain allOf')
    for now - use the size to see if all found
     */
    val expected5 = scored.filter( x => x match {
      case(Posting(1, 6,   None, None, 140, Some("CSS")),  67) => true
      case(Posting(1, 42,  None, None, 155, Some("PHP")),  89) => true
      case(Posting(1, 72,  None, None, 16,  Some("Ruby")), 3) => true
      case(Posting(1, 126, None, None, 33,  Some("Java")), 30) => true
      case(Posting(1, 174, None, None, 38,  Some("C#")),   20) => true
      case _ => false
    })
    expected5.collect.size should be === 5 //thanks to OP for the test hint


    /*
    Test 2 (continues data flow started in Test 1)
     */
    val vector = StackOverflow.vectorPostings(scored)
    val expectedInVector = vector.map( x => x match {
      case (350000, 67) => 1
      case (100000, 89) => 2
      case (300000, 3) => 4
      case (50000,  30) => 8
      case (200000, 20) => 16
      case _ => 0
    })
    expectedInVector.fold(0)((x, y) => x | y) should be === 31 //those values should present (thanks to OP for the hint on the test)

  }


//  "groupedPostings works" should {
//    "for empty input" in withSparkContext { (sparkContext:SparkContext) =>
//
//      /*
//      test sample from the original resource
//       */
//      val sampleRowStrings1 = "1,27233496,,,0,C#\n" +
//        "1,23698767,,,9,C#\n" +
//        "1,5484340,,,0,C#\n" +
//        "2,5494879,,5484340,1,\n" +
//        "1,9419744,,,2,Objective-C\n" +
//        "1,26875732,,,1,C#\n" +
//        "1,9002525,,,2,C++\n" +
//        "2,9003401,,9002525,4,\n" +
//        "2,9003942,,9002525,1,\n" +
//        "2,9005311,,9002525,0,"
//
//      val data = StackOverflow.rawPostings(sparkContext.parallelize(sampleRowStrings1.split("\n")))
//
//      val data:Seq[Posting] = Seq()
//
//      val rdd:RDD[Posting] = sparkContext.parallelize(data)
//      val total = rdd.map(...).filter(...).map(...).reduce(_ + _)
//
//      total shouldBe 1000
//    }
//  }


}

trait SparkContextSetup {
  def withSparkContext(testMethod: (SparkContext) => Any) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Spark test")
    val sparkContext = new SparkContext(conf)
    try {
      testMethod(sparkContext)
    }
    finally sparkContext.stop()
  }
}

