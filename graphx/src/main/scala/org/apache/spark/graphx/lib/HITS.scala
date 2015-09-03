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
import scala.math._

import org.apache.spark.Logging
import org.apache.spark.graphx._

/**
 * HITS (Hyperlink-Induced Topic Search) algorithm implementation..
 *
 * The implementation uses the standalone [[Graph]] interface and runs HITS
 * for a fixed number of iterations:
 * {{{
 * var Auth = Array.filll(n)(1.0)
 * var Hub  = Array.fill(n)(1.0)
 * var oldAuth = Array.fill(n)(1.0)
 * var oldHub = Array.fill(n)(1.0)

 * for( iter <- 0 until numIter) {
 *   swap(oldAuth, Auth)
 *   swap(oldHub, Hub)
 *   for( i <- 0 until n) {
 *     Auth[i] = inNbrs[i].map(j => oldHub[j]).sum
 *   }
 *   val totalAuth = sqrt(Auth.map(v => v*v).sum)

 *   for( i <- 0 until n) {
 *     Hub[i] = outNbrs[i].map(j => Auth[j]).sum
 *   }
 *   val totalHub = sqrt(Hub.map(v => v*v).sum)

 *   Auth.map(v => v/totalAuth)
 *   Hub.map(v => v/totalHub)
 *   }
 * }
 * }}}
 *
 * inNbrs[i] is the set of neighbors whick link to `i` and outNbrs[i]
 * is the set of neighbors which i link to.
 *
 */

case class Score(authority: Double, hub: Double)

object HITS extends Logging {

  /**
   * Run HITS for a fixed number of iterations returning a graph
   * with vertex attributes containing the authority score and hub score.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute using HITS algorithm
   * @param numIter the number of iterations of HITS to run
   *
   * @return the graph containing with each vertex containing the authority score and hub score
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int): Graph[Score, ED] =
  {
    // Initialize the HITS graph with each edge attribute having
    // each vertex with attribute of (authority=1.0, hub =1.0).
    var hitsGraph: Graph[Score, ED] = graph
      // Set the vertex attributes to the initial values
      .mapVertices( (id, attr) => Score(1.0, 1.0) )

    // cache the edges of HITS graph;
    // The edges are unchanged for HITS algorithm
    hitsGraph.edges.cache()

    var iteration = 0
    var preHITSVertices: VertexRDD[Score] = null
    while (iteration < numIter) {
      hitsGraph.vertices.cache()

      // update authority values by computing the incoming hub contributions of each vertex,
      // perform local preaggregation, and do the final aggregation at the receiving vertices.
      // Requires a shuffle for aggregation.
      val authUpdates = hitsGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr.hub), _ + _, TripletFields.Src)

      preHITSVertices = hitsGraph.vertices

      // apply authority updates to get the new scores, using outerJoin to fill 0 for vertices
      // that didn't receive message.
      hitsGraph = hitsGraph.outerJoinVertices(authUpdates) {
        (id, oldScore, msgOpt) => Score(msgOpt.getOrElse(0.0), oldScore.hub)
      }
      hitsGraph.vertices.cache()

      preHITSVertices.unpersist(false)

      // update hub values by computing the outgoing authority contributions of each vertex,
      // perform local preaggregation, and do the final aggregation
      // at the receiving vertices. Requires a shuffle for aggregation.
      val hubUpdates = hitsGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr.authority), _ + _, TripletFields.Dst)

      preHITSVertices = hitsGraph.vertices

      // apply hub updates to get new scores, using outerJoin to fill 0 for vertices
      // that didn't receive message
      hitsGraph = hitsGraph.outerJoinVertices(hubUpdates) {
        (id, oldScore, msgOpt) => Score(oldScore.authority, msgOpt.getOrElse(0.0))
      }
      hitsGraph.vertices.cache()

      preHITSVertices.unpersist(false)

      // Normalize the values by dividing each Hub score
      // by square root of the sum of the squares of all Hub scores,
      // and dividing each Authority score by square root of the sum of
      // the squares of all Authority scores.
      val normalizeAuth: Double = sqrt(hitsGraph.vertices
        .map{ case (id, score) => score.authority * score.authority}.sum)
      val normalizeHub: Double = sqrt(hitsGraph.vertices
        .map{ case (id, score) => score.hub * score.hub}.sum)

      preHITSVertices = hitsGraph.vertices

      hitsGraph = hitsGraph.mapVertices((id, attr) =>
        Score(attr.authority/normalizeAuth, attr.hub/normalizeHub))
      hitsGraph.vertices.cache()

      logInfo(s"HITS finished iteration $iteration.")
      preHITSVertices.unpersist(false)

      iteration += 1
    }

    hitsGraph
  }

}
