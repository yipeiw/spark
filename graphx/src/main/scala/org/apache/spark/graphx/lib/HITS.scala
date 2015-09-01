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

import scala.math._
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

case class Score(authority: Double, hub: Double)
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
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int): Graph[Score, ED] =
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
      graph: Graph[VD, ED], numIter: Int): Graph[Score, ED] =
  {
    // Initialize the HITS graph with each edge attribute having
    // each vertex with attribute of tuple(authority, hub) as (1.0, 1.0).
    var hitsGraph: Graph[Score, ED] = graph
      // Set the vertex attributes to the initial pagerank values
      .mapVertices( (id, attr) => Score(1.0, 1.0) )

    var iteration = 0
    var preHITSGraph: Graph[Score, ED] = null
    while (iteration < numIter) {
      hitsGraph.cache()

      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      //update authority values
      val authUpdates = hitsGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr.hub), _ + _, TripletFields.Src)

      preHITSGraph = hitsGraph

      //merge updated authority score into hitsGraph
      hitsGraph = hitsGraph.outerJoinVertices(authUpdates) {
        (id, oldScore, msgOpt) =>  Score(msgOpt.getOrElse(0.0), oldScore.hub)
      }.cache()

      hitsGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      preHITSGraph.vertices.unpersist(false)
      preHITSGraph.edges.unpersist(false)

      //update hub values
      val hubUpdates = hitsGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr.authority), _ + _, TripletFields.Dst)

      preHITSGraph = hitsGraph

      //merge updated hub score into hitsGraph
      hitsGraph = hitsGraph.outerJoinVertices(hubUpdates) {
        (id, oldScore, msgOpt) =>  Score(oldScore.authority, msgOpt.getOrElse(0.0))
      }.cache()

      hitsGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      preHITSGraph.vertices.unpersist(false)
      preHITSGraph.edges.unpersist(false)

      //calculate normalization factor
      val normalizeAuth: Double = sqrt(hitsGraph.vertices.map{ case (id,score) => square(score.authority)}.reduce((a,b) => a+b))
      val normalizeHub: Double = sqrt(hitsGraph.vertices.map{ case (id,score) => square(score.hub)}.reduce((a,b) => a+b))

      preHITSGraph = hitsGraph

      //reweighted the authority and hub score using the normalization factor
      hitsGraph.mapVertices((id, attr) => Score(attr.authority/normalizeAuth, attr.hub/normalizeHub))

      hitsGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      logInfo(s"HITS finished iteration $iteration.")
      preHITSGraph.vertices.unpersist(false)
      preHITSGraph.edges.unpersist(false)

      iteration += 1
    }

    hitsGraph
  }



}
