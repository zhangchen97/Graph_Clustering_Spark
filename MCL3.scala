
package org.jgi.spark.localcluster.tools
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.BlockMatrixFixedOps
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.graphx._
import java.io._

import com.typesafe.scalalogging.LazyLogging
import sext._
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph}
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

import scala.io.Source
import scala.reflect.ClassTag
import scala.reflect.io.Path
import scala.util.Try
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.jgi.spark.localcluster.DNASeq
import scopt.OptionParser
/**
  * MCL clustering algorithm.
  *
  * Clusters the graph according to: http://micans.org/mcl/
  *
  *
  */
object MCL3 extends App with LazyLogging {

	case class Config (input: String = "tmp/graph_gen_test.txt",//graph_gen_test.txt", // required
    output: String = "tmp/resultMCL_sample222", // default value
    partitions: Int = 100,
    checkpointEnable: Boolean = false,
    checkpointPath: String = "tmp/checkpoints_gen",
    expansionRate: Int = 2,
    inflationRate: Double =2.0,
	  epsilon: Double = 0.01,
	  maxIterations: Int = 10,
	  reindexNodes: Boolean = true,
	  blockSize: Int = 1024,
  	repartitionMatrix: Int = 100,
	  pruneEarly: Double = 0,
	  selfLoopWeight: Double = 1,
	  debugMode: Boolean = true,
	  secondDifference: Boolean = true,
	  localCalculations: Boolean = false
  )
  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("Spark-MCL") {
      head("Spark-MCL", "1.0")
      opt[String]('i', "input").required().valueName("<input-path>").
        action((x, c) => c.copy(input = x)).
        text("Setting Input is required")
      opt[String]('o', "output").action((x, c) =>
        c.copy(output = x)).text("output path is a string with a default of /tmp/resultMCL")
      opt[Int]('p', "partitions").action((x, c) =>
        c.copy(partitions = x)).text("partitions is an integer with default of 100")
      opt[Boolean]("checkpointEnable").action((x, c) =>
        c.copy(checkpointEnable = x)).text("checkpointEnable is boolean with default value false")
      opt[String]("checkpointPath").action((x, c) =>
        c.copy(checkpointPath = x)).text("checkpointPath is String with default path /tmp/checkpoints")
      opt[Int]("expansionRate").action((x, c) =>
        c.copy(expansionRate = x)).text("expansionRate is an integer with default value of 2")
      opt[Double]("inflationRate").action((x, c) =>
        c.copy(inflationRate = x)).text("inflationRate is Double with default value of 2.0")
      opt[Double]("epsilon").action((x, c) =>
        c.copy(epsilon = x)).text("epsilon is Double with default value of 0.01")
      opt[Int]("maxIterations").action((x, c) =>
        c.copy(maxIterations = x)).text("maxIterations is an integer with default value of 10")
      opt[Boolean]("reindexNodes").action((x, c) =>
        c.copy(reindexNodes = x)).text("reindexNodes is boolean with default value true")
      opt[Int]("blockSize").action((x, c) =>
        c.copy(blockSize = x)).text("blockSize is an integer with default value of 1024")
      opt[Int]("repartitionMatrix").action((x, c) =>
        c.copy(repartitionMatrix = x)).text("repartitionMatrix is Int with default value of 100")
      opt[Double]("pruneEarly").action((x, c) =>
        c.copy(pruneEarly = x)).text("pruneEarly is Double with default value of 0")
      opt[Double]("selfLoopWeight").action((x, c) =>
        c.copy(selfLoopWeight = x)).text("selfLoopWeight is Double with default value of 1")
      opt[Boolean]("debugMode").action((x, c) =>
        c.copy(debugMode = x)).text("debugMode is boolean with default value true")
      opt[Boolean]("secondDifference").action((x, c) =>
        c.copy(secondDifference = x)).text("secondDifference is boolean with default value true")
      opt[Boolean]("localCalculations").action((x, c) =>
        c.copy(localCalculations = x)).text("localCalculations is boolean with default value false")

    }
    parser.parse(args, Config())
  }

  override def main(args: Array[String]): Unit = {

    var t0 = System.nanoTime()
    val options =parse_command_line(args)
    println(s"called with arguments\n${options.valueTreeString}")
    options match {

      case Some(config) =>
        val conf = new SparkConf()
        conf.setAppName("MCLSpark").registerKryoClasses(Array(classOf[DNASeq]))
        println("xxxx")
        val sc = new SparkContext(conf)
        val rootLogger = Logger.getRootLogger()
        rootLogger.setLevel(Level.ERROR)

	val sqlContext: SQLContext = new HiveContext(sc)
    import sqlContext.implicits._
     if(config.checkpointEnable){
   sc.setCheckpointDir(config.checkpointPath)
    }
    var repart: Option[Int]=None
    if(config.repartitionMatrix>0){
    repart=Some(config.repartitionMatrix)
    }


    val txt = sc.textFile(config.input,config.partitions)
    val idMap = sc.broadcast(txt 
  .flatMap{line =>
    val lineArray = line.split(",|\\s+")
    if (lineArray.length < 2) {
      None
    } else {
        Seq(lineArray(0),lineArray(1))
    }
  }.distinct
  .zipWithIndex  
  .map{case (k, v) => (k, v.toLong)} 
  .collectAsMap)

    val triplets=txt.flatMap { line =>
        if (!line.isEmpty && line(0) != '#') {
    val lineArray = line.split(",|\\s+")
    if (lineArray.length < 2) {
      None
    } else {
      val t = new EdgeTriplet[String, String]
      t.srcId = idMap.value(lineArray(0))
      t.srcAttr = lineArray(0)
      t.attr = ((lineArray(2).toDouble)).toString
      t.dstId = idMap.value(lineArray(1))
      t.dstAttr = lineArray(1)
      Some(t)
    }
  } else {
    None
  }
}
txt.unpersist()
val vertices = triplets.flatMap(t => Array((t.srcId, t.srcAttr), (t.dstId, t.dstAttr)))
val edges: RDD[Edge[Double]] = triplets.map(t => Edge((t.srcId).toLong,(t.dstId).toLong,(t.attr).toDouble))
triplets.unpersist()
val g=Graph(vertices, edges)
vertices.unpersist()
edges.unpersist()
g.triplets.take(3).foreach(println)


val assignments = MCL3.run(g,
  expansionRate = config.expansionRate, inflationRate = config.inflationRate,
  epsilon = config.epsilon, maxIterations = config.maxIterations,
  rePartitionMatrix = repart, reIndexNodes = config.reindexNodes,
  blockSize = config.blockSize, pruneEarly = config.pruneEarly,
  selfLoopWeight =config.selfLoopWeight , debugMode= config.debugMode,
  checkpointEnable = config.checkpointEnable, secondDifference = config.secondDifference,
  localCalculations = config.localCalculations )
    assignments.take(10).foreach(println)



val look: DataFrame= g.vertices.toDF("id","name")
val ang1 = assignments.toDF("matrixId", "clusterId")
val realnames: RDD[(String,VertexId)]= ang1.join(look, ang1.col("matrixId")===look.col("id")).select($"name", $"clusterId").rdd.map(row => (row.getString(0), row.getLong(1)))
val ang = realnames.toDF("matrixId", "clusterId")
ang.printSchema()
    val data=ang.groupBy("clusterId")
    .agg(count("clusterId"),sort_array(collect_list(col("matrixId"))))
    .sort(desc("count(clusterId)"))
    data.printSchema()
    data.take(5)
    val finaldata = data
    .select($"sort_array(collect_list(matrixId), true)")
    //.collect()


val clustersN=ang.groupBy("clusterId").agg(sort_array(collect_list(col("matrixId")))).count()
println(s"Clusters Number: $clustersN" + "\n")

var t1 = System.nanoTime()
var seconds = (t1 - t0) / 1000000000.0;
println("Total Time Elapsed: " + seconds + " s")

        finaldata.rdd.repartition(1).saveAsTextFile(config.output)

//val file = config.output
//val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
//for (x <- finaldata) {
//  writer.write(x.getList(0) + "\n")  // however you want to format it
//}
//writer.close()
//
case None =>
}

  }

  def run[VD](graph: Graph[VD, Double],
              expansionRate: Int = 2,
              inflationRate: Double = 2.0,
              epsilon: Double = 0.01,
              maxIterations: Int = 10,
              reIndexNodes: Boolean = true,
              blockSize: Int = 1024,
              rePartitionMatrix: Option[Int] = Some(100),
              pruneEarly: Double = 0,
              selfLoopWeight: Double = 1,
              debugMode: Boolean = true,
              checkpointEnable: Boolean = false,
              secondDifference: Boolean = true,
              localCalculations: Boolean = false

             ): RDD[(VertexId, VertexId)] = {

    new MCL3(expansionRate, inflationRate, epsilon, maxIterations).run(graph, reIndexNodes, blockSize, rePartitionMatrix, pruneEarly, selfLoopWeight, debugMode, checkpointEnable, secondDifference, localCalculations)

  }
}

class MCL3 private(val expansionRate: Int,
                  val inflationRate: Double,
                  val epsilon: Double,
                  val maxIterations: Int)  extends Serializable {//with Logging {

  def normalization(mat: IndexedRowMatrix): IndexedRowMatrix = {
    new IndexedRowMatrix(
      mat.rows
        .map { row =>
          val svec = row.vector.toSparse
          val sumValues = svec.values.sum
          IndexedRow(row.index,
            new SparseVector(svec.size, svec.indices, svec.values.map(v => v / sumValues)))
        })
  }

  def inflation(mat: IndexedRowMatrix): IndexedRowMatrix = {

    new IndexedRowMatrix(
      mat.rows
        .map { row =>
          val svec = row.vector.toSparse
          IndexedRow(row.index,
            new SparseVector(svec.size, svec.indices, svec.values.map(v => Math.exp(inflationRate * Math.log(v)))))
        }
    )
  }


  def normalizationL(row: SparseVector): SparseVector ={
    new SparseVector(row.size, row.indices, row.values.map(v => v/row.values.sum))
  }

  def removeWeakConnectionsL(row: SparseVector, eps: Double): SparseVector ={
    new SparseVector(
      row.size,
      row.indices,
      row.values.map(v => {
        if(v < eps) 0.0
        else v
      })
    )
  }

  def inflationL(mat: BlockMatrix, eps: Double): IndexedRowMatrix = {

    new IndexedRowMatrix(
      //Bug 2.1.1 Needs to be Fixed - Partially Fixed by Uzadude and implemented in BlockMatrixFixedOps.toIndexedRowMatrixNew(BlockMatrix)
      //Till performance is better i would suggest mat.toCoordinateMatrix().toIndexedRowMatrix()
      //mat.toIndexedRowMatrix.rows
      mat.toCoordinateMatrix().toIndexedRowMatrix.rows
      //BlockMatrixFixedOps.toIndexedRowMatrixNew(mat).rows
        .map{row =>
          val svec = removeWeakConnectionsL(row.vector.toSparse, eps)
          IndexedRow(row.index,
           normalizationL(
              new SparseVector(svec.size, svec.indices, svec.values.map(v => Math.exp(inflationRate*Math.log(v))))
            )
          )
        }
    )
  }


  def removeWeakConnections(mat: IndexedRowMatrix,eps: Double): IndexedRowMatrix = {
    new IndexedRowMatrix(
      mat.rows.map { row =>
        val svec = row.vector.toSparse
        var numZeros = 0
        var mass = 0.0
        val svecNewValues = svec.values.map(v => {
          if (v < eps) {
            numZeros += 1
            0.0
          } else {
            mass += v
            v
          }
        })

        val REC_MIN_MASS = 0.85
        val REC_MIN_NNZ = math.round(1 / eps * 0.14).toInt
        val nnz = svec.indices.size - numZeros

        // in case we pruned too much
        if (nnz < REC_MIN_NNZ && mass < REC_MIN_MASS) {
          val (inds, vals) = (svec.indices zip svec.values).sortBy(-_._2).take(REC_MIN_NNZ).sorted.unzip
          IndexedRow(row.index, new SparseVector(svec.size, inds.toArray, vals.toArray))
        } else
          IndexedRow(row.index, new SparseVector(svec.size, svec.indices, svecNewValues))
      }
    )
  }


  def diff(m1: IndexedRowMatrix, m2: IndexedRowMatrix): Double = {

    val m1RDD: RDD[(Long, SparseVector)] = m1.rows.map((row: IndexedRow) => (row.index, row.vector.toSparse))
    val m2RDD: RDD[(Long, SparseVector)] = m2.rows.map((row: IndexedRow) => (row.index, row.vector.toSparse))

    m1RDD.join(m2RDD).map((tuple: (VertexId, (SparseVector, SparseVector))) => if (tuple._2._1 == tuple._2._2) 0 else 1).sum
  }

  def difference(m1: IndexedRowMatrix, m2: IndexedRowMatrix): Double = {

    val m1RDD:RDD[((Long,Int),Double)] = m1.rows.flatMap(r => {
      val sv = r.vector.toSparse
      sv.indices.map(i => ((r.index,i), sv.apply(i)))
    })

    val m2RDD:RDD[((Long,Int),Double)] = m2.rows.flatMap(r => {
      val sv = r.vector.toSparse
      sv.indices.map(i => ((r.index,i), sv.apply(i)))
    })

    val diffRDD = m1RDD.fullOuterJoin(m2RDD).map(diff => Math.pow(diff._2._1.getOrElse(0.0) - diff._2._2.getOrElse(0.0), 2))
    diffRDD.sum()
  }

  def run[VD](graph: Graph[VD, Double], reIndexNodes: Boolean, blockSize: Int, rePartitionMatrix: Option[Int], pruneEarly: Double, selfLoopWeight: Double, debugMode: Boolean, checkpointEnable: Boolean, secondDifference: Boolean, localCalculations: Boolean): RDD[(VertexId,VertexId)] = {

    val initTime=System.nanoTime()
      val sqlContext= new SQLContext(graph.vertices.sparkContext)
  import sqlContext.implicits._

    val lookupTable: DataFrame =
      graph.vertices.sortBy(_._1).zipWithIndex()
        .map(indexedVertex =>
          if (reIndexNodes)
            (indexedVertex._2.toInt, indexedVertex._1._1.toInt)
          else
            (indexedVertex._1._1.toInt, indexedVertex._1._1.toInt)
        )
        .toDF("matrixId", "nodeId")
        .cache()

    val preprocessedGraph: Graph[Int, Double] = MCLUtil.preprocessGraph(graph, lookupTable)
    graph.unpersist()
    val mat = MCLUtil.toIndexedRowMatrix(preprocessedGraph,selfLoopWeight, rePartitionMatrix)
    if(localCalculations==true){
    println(s"First Matrix: $mat.numRows()")}
    preprocessedGraph.unpersist()
    var iter = 0
    var change = 1.0
    var change1 = 1.0
    var blocksz=blockSize
    var M1: IndexedRowMatrix = null
    var M2: IndexedRowMatrix = null
    if(pruneEarly==0){
    println("Not Prunning Early")
    M1 = normalization(mat)

  }
    else{
    println(s"Prunning Early: $pruneEarly")
    M1= normalization(removeWeakConnections(normalization(mat),pruneEarly))
    println(s"Epsilon back to: $epsilon")

    }
    while (iter < maxIterations && change > 0) {
var t0 = System.nanoTime()
      println("================================================  iter " + iter + "  ================================================")
      if(iter==5){
        if(blocksz<10000){
        blocksz=10000 }
        else{
        blocksz=blocksz
        }
    }
    else if(iter==10){
        if(blocksz<20000){
        blocksz=20000}
        else{
        blocksz=blocksz+5000}
    }
    else if(iter==15){
        if(blocksz<30000){
        blocksz=30000}
        else{
        blocksz=blocksz+5000}
    }
    println(s"Blocksize: $blocksz")
      if(debugMode) {
      println("Num of Non-Zeros in Matrix: " + M1.rows.map((row: IndexedRow) =>
        row.vector.toSparse.indices.length ).sum)}

      if (localCalculations){
      println("Using the Local Prunning-Inflation-Normalization Mode")
      M2 = inflationL(MCLUtil.expansionL(M1, expansionRate, blocksz),epsilon)
    }
    else{
      println("Using the Distributed Prunning-Inflation-Normalization Mode")
      M2 = normalization(inflation(removeWeakConnections(MCLUtil.expansion(M1, expansionRate, blocksz, debugMode),epsilon)))
    }
      if(checkpointEnable){
      println ("CHECKPOINTING ENABLED")
      M2.rows.cache().checkpoint()
    }
    else{M2.rows.cache()}

      if(iter>=4) {
      if(secondDifference){
      change1 = difference(M1, M2)
      change = "%.5f".format(change1).toDouble}
      else{
        change1 = diff(M1, M2)
        change = "%.5f".format(change1).toDouble
      }
      println(s"MCL diff is: $change")
    }
    else{println("NOT CHECKING IF CONVERGING YET. TOO EARLY")}
    if(iter==5){
      val nr=M2.numRows()
      val nc=M2.numCols()
      val sz=(M1.rows.partitions.size/2).toInt
      M1.rows.unpersist()
      if (sz>4 && !rePartitionMatrix.isEmpty){
      M1=new IndexedRowMatrix(M2.rows.repartition(sz), nRows = nr, nCols = nc.toInt)
      println("REPARTIONED1. Old partitions: ")
      println(M2.rows.partitions.size)
      }
      else{println("NOT REPARTIONING1 BECAUSE OF LOW NUMBER IN PARTITIONS")}
    }
    else if(iter==10){
      val nr=M2.numRows()
      val nc=M2.numCols()
      val sz=(M1.rows.partitions.size/2).toInt
      M1.rows.unpersist()
      if (sz>4 && !rePartitionMatrix.isEmpty){
      M1=new IndexedRowMatrix(M2.rows.repartition(sz), nRows = nr, nCols = nc.toInt)
      println("REPARTIONED2. Old partitions: ")
      println(M2.rows.partitions.size)
      }
      else{println("NOT REPARTIONING2 BECAUSE OF LOW NUMBER IN PARTITIONS")}
    }
    else if(iter==16){
      val nr=M2.numRows()
      val nc=M2.numCols()
      val sz=(M1.rows.partitions.size/2).toInt
      M1.rows.unpersist()
      if (sz>4 && !rePartitionMatrix.isEmpty){
      M1=new IndexedRowMatrix(M2.rows.repartition(sz), nRows = nr, nCols = nc.toInt)
      println("REPARTIONED3. Old partitions: ")
      println(M2.rows.partitions.size)
      }
      else{println("NOT REPARTIONING3 BECAUSE OF LOW NUMBER IN PARTITIONS")}
    }
    else{
      M1.rows.unpersist()
      M1 = M2
    }
      println(M1.rows.partitions.size)
      iter = iter + 1
var t1 = System.nanoTime()
var seconds = (t1 - t0) / 1000000000.0;
println("Elapsed time: " + seconds + " s")

    }

    val rawDF =
      M1.rows.flatMap(
        r => {
          val sv = r.vector.toSparse
          sv.indices.map(i => (r.index, (i, sv.apply(i))))
        }
      ).groupByKey()
       .map(node => (node._1, node._2.maxBy(_._2)._1))
       .toDF("matrixId", "clusterId")




    // Reassign correct ids to each nodes instead of temporary matrix id associated

    val assignmentsRDD: RDD[(VertexId, VertexId)] =
      rawDF
        .join(lookupTable, rawDF.col("matrixId")===lookupTable.col("matrixId"))
        .select($"nodeId", $"clusterId")
        .rdd.map(row => (row.getInt(0).toLong, row.getInt(1).toLong))

    var finalTime = System.nanoTime()
    var tseconds = (finalTime - initTime) / 1000000000.0;
    println("Total elapsed time: " + tseconds + " s")
    assignmentsRDD
  }

}

object MCLUtil {

  val sb = new StringBuilder
  var debugMode = true

  /** Print an adjacency matrix in nice format.
    *
    * @param mat an adjacency matrix
    */
  def displayMatrix(mat: IndexedRowMatrix, format: String = "%d:%.4f ", predicate: Long => Boolean = (_) => true, showFull: Boolean = false): Unit = {
    if (debugMode) {
      println()
      mat
        .rows.sortBy(_.index).collect()
        .foreach(row => {
          if (predicate(row.index)) {
            //printf("%3d => ", row.index)
            printToSB("%3d   ".format(row.index))
            if (showFull)
              row.vector.toArray.foreach(v => printf(format, v))
            else {
              val svec = row.vector.toSparse
              //svec.foreachActive((i, v) => printf("%d:%.3f, ", i, v))
              svec.foreachActive((i, v) => printToSB(format.format(i, v)))
            }
            //println()
            printToSB("\n")
          }
        })
    }

    def printToSB(str: String): Unit = {
      sb.append(str)
      print(str)
    }
  }


 /** Deal with self loop
    *
    * Add one when weight is nil and remain as it is otherwise
    *
    * @param graph original graph
    * @param selfLoopWeight a coefficient between 0 and 1 to influence clustering granularity and objective
    * @return an RDD of self loops weights and associated coordinates.
    */
  def selfLoopManager(graph: Graph[Int, Double], selfLoopWeight: Double): RDD[(Int, (Int, Double))] = {

    val graphWithLinkedEdges: Graph[Array[Edge[Double]], Double] =
      Graph(
        graph
          .collectEdges(EdgeDirection.Either),
        graph.edges
      )

    val selfLoop:RDD[(Int, (Int, Double))] =
      graph
      .triplets
      .filter(e => e.srcId==e.dstId && e.attr > 0)
      .map(e => (e.srcId, e.srcAttr))
      .fullOuterJoin(graph.vertices)
      .filter(join => join._2._1.isEmpty)
      .leftOuterJoin(graphWithLinkedEdges.vertices)
      .map(v =>
        (v._2._1._2.get,
          (v._2._1._2.get,
            v._2._2.getOrElse(Array(Edge(1.0.toLong, 1.0.toLong, 1.0))).map(e => e.attr).max*selfLoopWeight)
          )
      )

    selfLoop
  }



  /** Get a suitable graph for MCL model algorithm.
    *
    * Each vertex id in the graph corresponds to a row id in the adjacency matrix.
    *
    * @param graph       original graph
    * @param lookupTable a matching table with nodes ids and new ordered ids
    * @return prepared graph for MCL algorithm
    */
  def preprocessGraph[VD](graph: Graph[VD, Double], lookupTable: DataFrame): Graph[Int, Double] = {
    val newVertices: RDD[(VertexId, Int)] =
      lookupTable.rdd.map(
        row => (row.getInt(1).toLong, row.getInt(0))
      )

    Graph(newVertices, graph.edges)
      .groupEdges((e1, e2) => e1 + e2)
  }

  /** Transform a Graph into an IndexedRowMatrix
    *
    * @param graph original graph
    * @return a ready adjacency matrix for MCL process.
    * @todo Check graphOrientationStrategy choice for current graph
    */
  def toIndexedRowMatrix(graph: Graph[Int, Double], selfLoopWeight: Double,
                         rePartitionMatrix: Option[Int] = None) = {

    //Especially relationships values have to be checked before doing what follows
    val rawEntries: RDD[(Int, (Int, Double))] = graph.triplets.map(triplet => (triplet.srcAttr, (triplet.dstAttr, triplet.attr)))

    val numOfNodes: Int = graph.numVertices.toInt

    // add self vertices

    val selfLoop:RDD[(Int, (Int, Double))] = selfLoopManager(graph, selfLoopWeight)
    val entries:RDD[(Int, (Int, Double))] = rawEntries.union(selfLoop)

    val indexedRows = entries.groupByKey().map(e =>
      IndexedRow(e._1, Vectors.sparse(numOfNodes, e._2.toSeq))
    )

    if (rePartitionMatrix.isEmpty)
      new IndexedRowMatrix(indexedRows, nRows = numOfNodes, nCols = numOfNodes)
    else new IndexedRowMatrix(indexedRows.repartition(rePartitionMatrix.get), nRows = numOfNodes, nCols = numOfNodes)

  }

  def expansion(mat: IndexedRowMatrix, expansionRate: Int = 2, blockSize: Int, debugMode: Boolean): IndexedRowMatrix = {
    //mat.rows.take(5).foreach(println(_))
    println("blocksize count : "+blockSize)
    val bmat = mat.toBlockMatrix(blockSize, blockSize)
    var resmat = bmat
    for (i <- 1 until expansionRate) {
      resmat = BlockMatrixFixedOps.multiply(resmat, bmat)
    }
    //BUG in 2.x
    //val Myo=resmat.toIndexedRowMatrix()
    val Myo=resmat.toCoordinateMatrix().toIndexedRowMatrix()
    //val Myo=BlockMatrixFixedOps.toIndexedRowMatrixNew(resmat)
  if (debugMode){
println("Num of Non-Zeros in Matrix in expansion: " + Myo.rows.map((row: IndexedRow) => row.vector.toSparse.indices.length ).sum)}
Myo
}



def expansionL(mat: IndexedRowMatrix, expansionRate: Int = 2, blockSize: Int): BlockMatrix = {
    val bmat = mat.toBlockMatrix(blockSize, blockSize)
    var resmat = bmat
    for (i <- 1 until expansionRate) {
      resmat = BlockMatrixFixedOps.multiply(resmat, bmat)
    }
resmat
}

  /** Transform an IndexedRowMatrix into a Graph
    *
    * @param mat      an adjacency matrix
    * @param vertices vertices of original graph
    * @return associated graph
    */
  def toGraph(mat: IndexedRowMatrix, vertices: RDD[(VertexId, String)]): Graph[String, Double] = {
    val edges: RDD[Edge[Double]] =
      mat.rows.flatMap(f = row => {
        val svec: SparseVector = row.vector.toSparse
        val it: Range = svec.indices.indices
        it.map(ind => Edge(row.index, svec.indices.apply(ind), svec.values.apply(ind)))
      })
    Graph(vertices, edges)
  }
}
