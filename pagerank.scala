package org.jgi.spark.localcluster.tools

/***************************************************************************
 * PageRank calculation
 ***************************************************************************/
import org.apache.spark.rdd.RDD
import com.typesafe.scalalogging.LazyLogging

object PageRank extends App with LazyLogging
{
  /***************************************************************************
   * PageRank calculation
   * given graph and damping rate, calculate PageRank ergodic frequency
   ***************************************************************************/
  def apply(
             graph: Graph_self, damping: Double, errThFactor: Double): RDD[(Long,Double)] =
  {
    logger.info(s"Calculating PageRank\n")
    logger.info(s"PageRank teleportation probablity ${1-damping}\n")
    logger.info(s"PageRank error threshold factor $errThFactor\n")

    val nodeNumber: Long = graph.vertices.count
    val edges: Matrix = {
      //标准化后这是在干嘛？
      val outLinkTotalWeight = graph.edges.map {
        case (from,(to,weight)) => (from,weight)
      }.reduceByKey(_+_)
      outLinkTotalWeight.cache
      //outLinkTotalWeight.take(10).foreach(println(_))  输出：(25,1.0)  (1,1.0)  (17,1.0000000000000002)

      // nodes without outbound links are dangling"没有出站链接的节点悬而未决”
      val dangling: RDD[Long] = graph.vertices.leftOuterJoin(outLinkTotalWeight)
        .filter {
          case (_,(_,Some(_))) => false
          case (_,(_,None)) => true
        }.map {
        case (idx,_) => idx
      }
      // dangling nodes jump to uniform probability
      val constCol = dangling.map (
        x => ( x, 1.0/nodeNumber.toDouble ) )

      // normalize the edge weights
      val normMat = graph.edges.join(outLinkTotalWeight)
        .map {
          case (from,((to,weight),totalweight)) => (from,(to,weight/totalweight))
        }

      outLinkTotalWeight.unpersist()

      Matrix( normMat, constCol )
    }
    edges.sparse.cache
    edges.constCol.cache

    // start with uniform ergodic frequency
    val freqUniform = graph.vertices.map {
      case (idx,_) => ( idx, 1.0/nodeNumber.toDouble )
    }
    freqUniform.cache

    // calls inner PageRank calculation function
    PageRank( edges, freqUniform, nodeNumber, damping,
      1.0/nodeNumber.toDouble/errThFactor, 0)
  }

  /***************************************************************************
   * PageRank calculation
   * given initial ergodic frequency and edges
   * calculation terminates when consequtive iterations differ less than errTh
   ***************************************************************************/
  @scala.annotation.tailrec
  def apply(
             edges: Matrix, freq: RDD[(Long,Double)],
             n: Long, damping: Double, errTh: Double, loop: Long): RDD[(Long,Double)] =
  {
    // print the PageRank iteration number only in debug log
    logger.info(s"Calculating PageRank, iteration $loop\n")

    // 2D Euclidean distance between two vectors
    def dist2D( v1: RDD[(Long,Double)], v2: RDD[(Long,Double)] ): Double = {
      val diffSq = (v1 join v2).map {
        case (idx,(e1,e2)) => (e1-e2)*(e1-e2)
      }
        .sum
      Math.sqrt(diffSq)
    }

    // create local checkpoint to truncate RDD lineage
    freq.checkpoint
    freq.cache
    val forceEval = freq.count

    // the random walk contribution of the ergodic frequency
    val stoFreq = edges *freq
    // the random jump contribution of the ergodic frequency
    val bgFreq = freq.map {
      case (idx,_) => (idx, (1.0-damping)/n.toDouble )
    }

    // combine both random walk and random jump contributions
    val newFreq = (bgFreq leftOuterJoin stoFreq).map {
      case (idx,(bg,Some(sto))) => ( idx, bg+ sto*damping )
      case (idx,(bg,None)) => ( idx, bg )
    }
    newFreq.cache

    // recursive call until freq converges wihtin error threshold
    val err = dist2D(freq,newFreq)

    if( err < errTh ) newFreq
    else PageRank( edges, newFreq, n, damping, errTh, loop+1 )
  }
}

/*****************************************************************************
 * A matrix class stored using sparse entries
 * this is used for calculating PageRank
 *****************************************************************************/

sealed case class Matrix
( sparse: RDD[(Long,(Long,Double))],
  constCol: RDD[(Long,Double)] )
  extends Serializable {
  def *( vector: RDD[(Long,Double)] ): RDD[(Long,Double)] = {

    // constCol is an optimization,
    // if all entries within a column has constant value
    val constColProd = (constCol join vector).map {
      case (from,(col,vec)) => col*vec
    }
      .sum

    val constColVec = vector.map {
      case (idx,x) => (idx,constColProd)
    }

    val matTimesVec = (sparse join vector).map {
      case (from,((to,matrix),vec)) => (to,vec*matrix)
    }
      .reduceByKey(_+_)

    val matTimesVecPlusConstCol = (matTimesVec rightOuterJoin constColVec)
      .map {
        case (idx,(Some(x),col)) => (idx,x+col)
        case (idx,(None,col)) => (idx,col)
      }

    matTimesVecPlusConstCol
  }
}
