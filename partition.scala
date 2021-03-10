package org.jgi.spark.localcluster.tools

/*****************************************************************************
 * store graph data that are relevant to community detection
 * which involves a few scalar (Double or Long) variables
 * and a graph (vertices and edges)
 * importantly, the graph stores reduced graph
 * where each node represents a module/community
 * this reduced graph can be combined with the original graph (Graph object)
 *****************************************************************************/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.jgi.spark.localcluster.tools.GraphInfomap.Config
//sealed 关键字主要有2个作用：
//1:其修饰的trait，class只能在当前文件里面被继承
//2:用sealed修饰这样做的目的是告诉scala编译器在检查模式匹配的时候，
// 让scala知道这些case的所有情况，scala就能够在编译的时候进行检查，
// 看你写的代码是否有没有漏掉什么没case到，减少编程的错误。
sealed case class Partition
(
  nodeNumber: Long, tele: Double,
  // | idx , n , p , w , q |
  vertices: RDD[(Long,(Long,Double,Double,Double))],
  // | index from , index to , weight |
  edges: RDD[(Long,(Long,Double))],
  // sum of plogp(ergodic frequency), for codelength calculation
  // this can only be calculated when each node is its own module
  // i.e. in Partition.init()
  probSum: Double,
  codelength: Double // codelength given the modular partitioning
)

/*****************************************************************************
 * given a Graph (probably from GraphFile.graph)
 * and the PageRank teleportation probability
 * calculate PageRank and exit probabilities for each node
 * these are put and returned to a Partition object
 * which can be used for community detection
 *****************************************************************************/

object Partition
{
  def init( graph: Graph_self, config: Config ): Partition = init(
    graph,
    config.tele,
    config.error_threshold_factor
  )
  def init(graph: Graph_self, tele: Double, errThFactor: Double): Partition = {
    //顶点数量
    val nodeNumber: Long = graph.vertices.count
    // filter away self-connections  过滤掉自连接
    // and normalize edge weights per "from" node  并且标准化每个“来自”节点的边缘权重
    val edges = {
      //将起点=终点的边过滤掉
      val nonselfEdges = graph.edges.filter {
        case (from,(to,weight)) => from != to }
      // 目的是将权重标准话
      val outLinkTotalWeight = nonselfEdges.map {
        case (from,(to,weight)) => (from,weight)}.reduceByKey(_+_)
      nonselfEdges.join(outLinkTotalWeight).map {
        case (from,((to,weight),norm)) => (from,(to,weight/norm))
      }
    }
    edges.cache

    // exit probability from each vertex
    val ergodicFreq = PageRank(Graph_self(graph.vertices,edges),
      1-tele, errThFactor)
    ergodicFreq.cache

    // modular information
    // since transition probability is normalized per "from" node,
    // w and q are mathematically identical to p
    // as long as there is at least one connection
    // | id , size , prob , exitw , exitq |
    val vertices: RDD[(Long,(Long,Double,Double,Double))] = {

      val exitw: RDD[(Long,Double)] = edges
        .join( ergodicFreq ).map {
        case (from,((to,weight),ergodicFreq)) => (from,ergodicFreq*weight)
      }.reduceByKey(_+_)

      // since exitw is normalized per "from" node,
      // exitw is always (from,freq)
      // so calculations can be simplified
      ergodicFreq.leftOuterJoin(exitw).map {
        //case (idx,(freq,Some(w))) => (idx,(1,freq,w,tele*freq+(1-tele)*w))
        case (idx,(freq,Some(_))) => (idx,(1,freq,freq,freq))
        case (idx,(freq,None))
        => if( nodeNumber > 1) (idx,(1,freq,0,tele*freq))
        else (idx,(1,1,0,0))
      }
    }
    val forceEval = vertices.count
    vertices.checkpoint
    vertices.cache

    val exitw = edges.join(ergodicFreq).map {
      case (from,((to,weight),freq)) => (from,(to,freq*weight))}
    exitw.cache

    val probSum = ergodicFreq.map {
      case (_,p) => CommunityDetection.plogp(p)
    }.sum

    ergodicFreq.unpersist()

    val codelength = CommunityDetection.calCodelength( vertices, probSum )

    // return Partition object
    Partition(nodeNumber, tele, vertices, exitw, probSum, codelength)
  }
}
