package net.sansa_stack.Data_Compression.spark.rdf

import java.net.URI

import scala.collection.mutable

import net.sansa_stack.rdf.spark.io._
import org.apache.spark.sql.SparkSession
import org.apache.jena.riot.Lang

object DataCompression {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String): Unit = {

    val spark = SparkSession.builder
      .appName(s"Data Compression  $input")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("======================================")
    println("|        RDF Data Compression 2018     |")
    println("======================================")

    val lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(input)

    triples.take(20).foreach(println(_))

    spark.stop

  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Data Compression") {

    head(" Data Compression")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    help("help").text("prints this usage text")
  }
}