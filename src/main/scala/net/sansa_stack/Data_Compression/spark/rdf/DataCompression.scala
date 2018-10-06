package net.sansa_stack.Data_Compression.spark.rdf

import java.net.URI

import scala.collection.mutable
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.graph.Node
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object DataCompression {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, output:String): Unit = {

    val spark = SparkSession.builder
      .appName(s"Data Compression  $input")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sql=spark.sqlContext;

    println("==========================================")
    println("|         RDF Data Compression 2018       |")
    println("==========================================")

    case class RDFTriples( subject: String, obj: String, predicate:String)
    val schemaTriple= StructType(
      Seq( StructField(name = "subject", dataType = StringType, nullable = false),
        StructField(name = "object", dataType = StringType, nullable = false),
        StructField(name = "predicate", dataType = StringType, nullable = false)));

    val schema= StructType(
      Seq( StructField(name = "name", dataType = StringType, nullable = false),
        StructField(name = "index", dataType = LongType, nullable = false)));

    val lang = Lang.NTRIPLES

    val triples: RDD[graph.Triple] = spark.rdf(lang)(input)

    val triplesDF =  spark.createDataFrame(triples.map(t=> Row(t.getSubject.toString,t.getObject.toString(),t.getPredicate.toString())),schemaTriple)
    val subjectDF= spark.createDataFrame(triples.map(_.getSubject.toString()).distinct().zipWithIndex().map(t=> Row(t._1,t._2)),schema).cache();
    val objectDF = spark.createDataFrame(triples.map(_.getObject.toString()).distinct().zipWithIndex().map(t=> Row(t._1,t._2)),schema).cache();
    val predicateDF = spark.createDataFrame(triples.map(_.getPredicate.toString()).distinct().zipWithIndex().map(t=> Row(t._1,t._2)),schema).cache();

    triplesDF.createOrReplaceTempView("triples");
    subjectDF.createOrReplaceTempView("subject");
    objectDF.createOrReplaceTempView("object");
    predicateDF.createOrReplaceTempView("predicate");

    println("Number of Triples:   " + triples.count())
    println("Number of subjects:   " + subjectDF.count())
    println("Number of predicates: " + predicateDF.count())
    println("Number of objects:    " + objectDF.count())

    val result= sql.sql("select subject.index as s_index,object.index as o_index, predicate.index as p_index from triples join subject on triples.subject=subject.name " +
      "join object on triples.object=object.name " +
      "join predicate on triples.predicate=predicate.name");

    println("Count after Transformation: "+result.count());
    result.write.csv(output+"triples")
    subjectDF.write.csv(output+"subject")
    objectDF.write.csv(output+"object")
    predicateDF.write.csv(output+"predicate")

    spark.stop



    //triples.saveAsTextFile("D:\\abakarb\\src\\main\\resources\\Output-Data\\DBPEDIA\\Text")



  }

  case class Config(in: String = "",out:String="",compressedInput:String="")

  val parser = new scopt.OptionParser[Config]("Data Compression") {

    head(" Data Compression")

    opt[String]('i', "input").optional().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")
    opt[String]('i', "compressed-input").optional().valueName("<path>").
      action((x, c) => c.copy(compressedInput = x)).
      text("path to compressed dir that contains the data.")
    opt[String]("output").required().valueName("<path>").
      action((x, c) => c.copy(out = x)).
      text("path to directory where compressed the data are written")

    help("help").text("prints this usage text")
  }
}