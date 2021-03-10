package org.jgi.spark.localcluster.tools

/*****************************************************************************
 * static function that delegates to PajekReader or ParquetReader
 * based on file extension
 *****************************************************************************/

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.jgi.spark.localcluster.tools.GraphInfomap.Config
//定义图类
sealed case class Graph_self
(
  vertices: RDD[(Long,(String,Long))], // | index , name , module |
  edges: RDD[(Long,(Long,Double))] // | index from , index to , weight |
)

object GraphReader
{
  def apply( sc: SparkContext,  config:Config): Graph_self = {
    //先读取顶点，然后读取边
    val graphver: RDD[(Long,(String,Long))] = ReadGraphver( sc, config )
    //graphver.repartition(1).saveAsTextFile("tmp/graphver3")
    val graphedg: RDD[(Long,(Long,Double))]=ReadGraphedg(sc,config)
    val graph=Graph_self(graphver,graphedg)
    graph.vertices.cache
    val force1 = graph.vertices.count
    println("vertices.count="+force1)
    graph.edges.cache
    val force2 = graph.edges.count
    println("edges.count="+force2)
    graph
  }


//  def ReadGraphver(sc: SparkContext, config: Config) ={
//
//    val verticessc=sc.textFile(config.reads_input)
//      .map(line=>line.split("\t|\n"))
//      .map(x=>(x(0).toLong,(x(2),x(0).toLong))  ).distinct()
//    verticessc
//  }
  def ReadGraphver(sc: SparkContext, config: Config) ={
//      val vertice1=sc.textFile(config.edge_file)
//        .map{
//          line=>line.split(",").map(_.take(2)).map(_.toLong)
//        }.flatMap(x=>x).distinct()
//
//      println("verticessc count: "+vertice1.count())
//      val verticessc=vertice1.map(x=>(x,(x.toString,x)))

    val vertice1=sc.textFile(config.edge_file,config.n_partition)
      .map(line=>line.split(","))
      .map(x=> (x(0).toLong,x(1).toLong))
    val vertice2=vertice1.map(x=>x.swap)
        //(x(0).toLong,(x(0)+"i",x(0).toLong))).distinct().sortBy(x=>x._1)
    val verticessc=vertice1.union(vertice2)
      .map(x=>(x._1,(x._1.toString(),x._1))).distinct().sortBy(x=>x._1)
    verticessc
  }

  def ReadGraphedg(sc: SparkContext, config: Config)={

    val edges=sc.textFile(config.edge_file,config.n_partition)
      .map(line=>line.split(","))
    //.filter(x => x(2).toInt >= config.min_shared_kmers && x(2).toInt <= config.max_shared_kmers.toInt)
      .map(x=>(x(0).toLong,(x(1).toLong,x(2).toDouble)))
    edges
  }

}

