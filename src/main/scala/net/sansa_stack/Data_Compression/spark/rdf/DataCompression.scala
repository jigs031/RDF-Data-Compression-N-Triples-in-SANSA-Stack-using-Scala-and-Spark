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

    //Initialized the spark session
    val spark = SparkSession.builder
      .appName(s"Data Compression  $input")
      .master("local[*]") //Run the Spark on local mode.  remove this line when run it in cluster mode
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //Initialize the SQL context to enable SQL based query on Spark RDDs
    val sql=spark.sqlContext;

    println("==========================================")
    println("|         RDF Data Compression 2018       |")
    println("==========================================")

    //Define the Schema for RDF RDDs
    val schemaTriple= StructType(
      Seq( StructField(name = "subject", dataType = StringType, nullable = false),
        StructField(name = "object", dataType = StringType, nullable = false),
        StructField(name = "predicate", dataType = StringType, nullable = false)));

    //Define Schema for Subject/Object/Predicate Dictionary
    val schema= StructType(
      Seq( StructField(name = "name", dataType = StringType, nullable = false),
        StructField(name = "index", dataType = LongType, nullable = false)));

    val lang = Lang.NTRIPLES

    //Read RDF dataset, vertical partition it and store it in Spark distributed RDD
    val triples: RDD[graph.Triple] = spark.rdf(lang)(input)

    //Create DataFrame (DF similar to RDBMS Table) by imposing Schema on Triple RDD.
    val triplesDF =  spark.createDataFrame(triples.map(t=> Row(t.getSubject.toString,t.getObject.toString(),t.getPredicate.toString())),schemaTriple)

    //Create the Dataframe for each dictionary.
    val subjectDF= spark.createDataFrame(triples.map(_.getSubject.toString()).distinct().zipWithIndex().map(t=> Row(t._1,t._2)),schema).cache();
    val objectDF = spark.createDataFrame(triples.map(_.getObject.toString()).distinct().zipWithIndex().map(t=> Row(t._1,t._2)),schema).cache();
    val predicateDF = spark.createDataFrame(triples.map(_.getPredicate.toString()).distinct().zipWithIndex().map(t=> Row(t._1,t._2)),schema).cache();

    //To enable SQL queries on top of distributed DF, register DFs as tables.
    triplesDF.createOrReplaceTempView("triples");
    subjectDF.createOrReplaceTempView("subject");
    objectDF.createOrReplaceTempView("object");
    predicateDF.createOrReplaceTempView("predicate");

    //Print the count of each DF
    println("Number of Triples:   " + triples.count())
    println("Number of Dictionary  subjects:   " + subjectDF.count())
    println("Number of Dictionary predicates: " + predicateDF.count())
    println("Number of Dictionary objects:    " + objectDF.count())

    //Create Fact table for Triples. Fact table contains unique ID of Subject/Object/Predicate
    val result= sql.sql("select subject.index as s_index,object.index as o_index, predicate.index as p_index from triples join subject on triples.subject=subject.name " +
      "join object on triples.object=object.name " +
      "join predicate on triples.predicate=predicate.name");

    //Validate the Count of Fact table
    println("Count after Transformation: "+result.count());

    //Store Dictionaries (Dimension tables) and Fact table on HDFS
    result.write.csv(output+"triples")
    subjectDF.write.csv(output+"subject")
    objectDF.write.csv(output+"object")
    predicateDF.write.csv(output+"predicate")


    //Performing Queries on Compressed RDDs
    var stime=System.currentTimeMillis()
    val result1= sql.sql("select subject.name as s_name,object.name as o_name, predicate.name as p_name from triples " +
      "join subject on triples.subject=subject.name and subject.name='http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1' " +
      "join object on triples.object=object.name and object.name='http://www.w3.org/1999/02/22-rdf-syntax-ns#type' " +
      "join predicate on triples.predicate=predicate.name").count();
    var etime=System.currentTimeMillis()
    println("Total Time: "+ (etime-stime) +" ms.")

    stime=System.currentTimeMillis()
    val result2= sql.sql("select subject.name as s_name,object.name as o_name, predicate.name as p_name from triples " +
      "join subject on triples.subject=subject.name and subject.name='http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1' " +
      "join object on triples.object=object.name and object.name='http://www.w3.org/2000/01/rdf-schema#label' " +
      "join predicate on triples.predicate=predicate.name").count;
    etime=System.currentTimeMillis()
    println("Total Time: "+ (etime-stime) +" ms.")

    //stoping the Spark
    spark.stop



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