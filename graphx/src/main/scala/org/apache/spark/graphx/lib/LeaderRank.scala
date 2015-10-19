import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object LeaderRank {
def LeaderRank(graph: Graph[Int, Int]):Graph[Double, Double] = {
    val pagerankGraph: Graph[(Double, Double), Double] = graph.outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }.mapTriplets(e => 1.0 / e.srcAttr).mapVertices((id, attr) => (0.0, 0.0))
    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      if (msgSum != 0.0) {
        (msgSum, Math.abs(oldPR - msgSum))
      } else {
        if (id == 0) {
          (0.0, 0.0)
        } else {
          (1.0, 0.0)
        }
      }
    }
    def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
      Iterator((edge.dstId, edge.srcAttr._1 * edge.attr))
    }
    def messageCombiner(a: Double, b: Double): Double = a + b
    val rank = Pregel(pagerankGraph, 0.0, 15, activeDirection = EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner).mapVertices(((id, attr) => attr._1))
    val ground = (rank.vertices.filter(_._1 == 0).collect()(0)._2) / (rank.numVertices - 1)
    val result = rank.mapVertices((id, attr) => attr + ground)
    result
  }
}
