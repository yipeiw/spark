/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.lib

import scala.reflect.ClassTag
import scala.language.postfixOps

import org.apache.spark.Logging
import org.apache.spark.graphx._

/**
 * HITS algorithm implementation. There are two implementations of HITS implemented.
 *
 * The implementation uses the standalone [[Graph]] interface and runs HITS
 * for a fixed number of iterations:
 * {{{
 * }}}
 *
 * neighbors whick link to `i` and `outDeg[j]` is the out degree of vertex `j`.
 *
 * Note that this is not the "normalized" HITS and as a consequence pages that have no
 * inlinks will have a HITS of alpha.
 */

object HITS extends Logging {


  /**
   * Run HITS for a fixed number of iterations returning a graph
   * with vertex attributes containing the HITS and edge
   * attributes the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute HITS
   * @param numIter the number of iterations of HITS to run
   *
   * @return the graph containing with each vertex containing the HITS
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int): Graph[(Double, Double), String] =
  {
    runWithOptions(graph, numIter)
  }

  /**
   * Run HITS for a fixed number of iterations returning a graph
   * with vertex attributes containing the HITS and edge.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute HITS
   * @param numIter the number of iterations of HITS to run
   * @param srcId the source vertex for a Personalized Page Rank (optional)
   *
   * @return the graph containing with each vertex containing the HITS
   */
  def runWithOptions[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int): Graph[(Double, Double), String] =
  {
    //To do
    // Initialize the HITS graph with each edge attribute having
    // each vertex with attribute 1.0.
    /*var rankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices( (id, attr) => resetProb )

    val personalized = srcId isDefined
    val src: VertexId = srcId.getOrElse(-1L)
    def delta(u: VertexId, v: VertexId): Double = { if (u == v) 1.0 else 0.0 }

    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < numIter) {
      rankGraph.cache()

      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
      // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

      // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
      // edge partitions.
      prevRankGraph = rankGraph
      val rPrb = if (personalized) {
        (src: VertexId , id: VertexId) => resetProb * delta(src, id)
      } else {
        (src: VertexId, id: VertexId) => resetProb
      }

      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => rPrb(src, id) + (1.0 - resetProb) * msgSum
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      logInfo(s"HITS finished iteration $iteration.")
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }*/

    var HITSGraph: Graph[(Double, Double), String] = null
    HITSGraph
  }



}
