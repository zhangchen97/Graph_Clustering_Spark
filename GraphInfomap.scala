package org.jgi.spark.localcluster.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster.Utils
import org.jgi.spark.localcluster.tools.GraphGen2.logger
import sext._

object GraphInfomap extends App with LazyLogging{
  case class Config(edge_file: String = "", output: String = "", min_shared_kmers: Int = 2,
                    max_iteration: Int = 10, n_output_blocks: Int = 100, min_reads_per_cluster: Int = 2,
                    max_shared_kmers: Int = 20000, sleep: Int = 0,
                    n_partition: Int = 0, tele:Double= 0 , error_threshold_factor:Double= 0,
                    merge_direction:String="" )

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("GraphInfomap") {
      head("GraphInfomap", Utils.VERSION)

      opt[String]('i', "edge_file").required().valueName("<file>").action((x, c) =>
        c.copy(edge_file = x)).text("files of graph edges. e.g. output from GraphGen")

      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output file")

      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("paritions for the input")


      opt[Int]("max_iteration").action((x, c) =>
        c.copy(max_iteration = x))
        .text("max ietration for LPA")

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")


      opt[Int]("n_output_blocks").action((x, c) =>
        c.copy(n_output_blocks = x)).
        validate(x =>
          if (x >= 1) success
          else failure("n_output_blocks should be greater than 0"))
        .text("output block number")

      opt[Int]("min_shared_kmers").action((x, c) =>
        c.copy(min_shared_kmers = x)).
        validate(x =>
          if (x >= 0) success
          else failure("min_shared_kmers should be greater than 2"))
        .text("minimum number of kmers that two reads share")

      opt[Int]("max_shared_kmers").action((x, c) =>
        c.copy(max_shared_kmers = x)).
        validate(x =>
          if (x >= 1) success
          else failure("max_shared_kmers should be greater than 1"))
        .text("max number of kmers that two reads share")


      opt[Int]("min_reads_per_cluster").action((x, c) =>
        c.copy(min_reads_per_cluster = x))
        .text("minimum reads per cluster")
      //tele
      opt[Double]("tele").action((x, c) =>
        c.copy(tele = x))
        .text("tele")
      //error threshold factor
      opt[Double]("error_threshold_factor").action((x, c) =>
        c.copy(error_threshold_factor = x))
        .text("error threshold factor")

      //merge_direction
      opt[String]("merge_direction").valueName("merge_direction").action((x, c) =>
        c.copy(merge_direction = x))
        .text("merge_direction")

      help("help").text("prints this usage text")

    }
    parser.parse(args, Config())
  }

  /***************************************************************************
   * read in graph
   ***************************************************************************/
  def readGraph( sc: SparkContext, config: Config): Graph_self = {
    val graph = GraphReader( sc, config)
    val vertices = graph.vertices.count
    val edges = graph.edges.count
    logger.info(s"Read in network with $vertices nodes and $edges edges\n")
    graph
  }
  /***************************************************************************
   * initialize partitioning
   ***************************************************************************/
  def initPartition( graph: Graph_self, config: Config): Partition = {
    logger.info(s"Initializing partitioning\n")
    val part = Partition.init( graph, config)
    logger.info(s"Finished initialization calculations\n")
    part
  }
  /***************************************************************************
   * perform community detection
   ***************************************************************************/
  def communityDetection( graph: Graph_self, part: Partition,config: Config): (Graph_self,Partition) = {
    val algoName = "InfoFlow"
    val algo = {
      if( algoName == "InfoFlow" )
        new InfoFlow(config)
      else throw new Exception(
        "Community detection algorithm must be InfoMap or InfoFlow"
      )
    }
    algo(graph,part)
  }
  /***************************************************************************
   * save final graph
   ***************************************************************************/
  def saveFinalGraph( graph: Graph_self, part: Partition, config: Config )
  : Unit = {
    logger.info(s"Save final graph\n")
    logger.info(s"with ${part.vertices.count} modules"
      +s" and ${part.edges.count} connections\n",
      false)
    val result=graph.vertices.map{
      case(id,(name,module))=>(module,id)
    }.groupByKey().map{
      case(module,iterator)=> iterator.toList.sorted.mkString(",")
    }

    KmerCounting.delete_hdfs_file(config.output)
    result.repartition(1).saveAsTextFile(config.output)
  }

  def checkpoint_dir = {
    System.getProperty("java.io.tmpdir")
  }

  def run(config: Config, sc: SparkContext): Unit = {
    val start = System.currentTimeMillis //此刻时间
    logger.info(new java.util.Date(start) + ": Program started ...")
    sc.setCheckpointDir(checkpoint_dir)
    val graph0: Graph_self = readGraph( sc, config)
    val part0: Partition = initPartition( graph0, config )
    val(graph1,part1) = communityDetection( graph0, part0, config )
    saveFinalGraph( graph1, part1, config )


    val totalTime1 = System.currentTimeMillis
    println("GraphInfoFlow time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))

  }

  override def main(args: Array[String]): Unit = {
    val APPNAME = "Spark GraphInfomap"
    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get

        logger.info(s"called with arguments\n${options.valueTreeString}")
        println(s"called with arguments\n${options.valueTreeString}")
        val conf = new SparkConf().setAppName(APPNAME)
        val sc = new SparkContext(conf)

        run(config,sc)
        if (config.sleep > 0) Thread.sleep(config.sleep * 1000)
        sc.stop()
      case None =>
        println("bad arguments")
        sys.exit(-1)
    }
  }
}
