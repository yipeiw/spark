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

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.util.GraphGenerators

import scala.math._ 

object GridHITS {
  def apply(nRows: Int, nCols: Int, nIter: Int): Seq[(VertexId, Score)] = {
    
    val inNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])
    val outNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])

    // Convert row column address into vertex ids (row major order)
    def sub2ind(r: Int, c: Int): Int = r * nCols + c
    // Make the grid graph
    for (r <- 0 until nRows; c <- 0 until nCols) {
      val ind = sub2ind(r, c)
      if (r + 1 < nRows) {
	val connectInd = sub2ind(r+1, c)
        outNbrs(ind) += connectInd
        inNbrs(connectInd) += ind
      }
      if (c + 1 < nCols) {
	val connectInd = sub2ind(r, c + 1)
        outNbrs(ind) += connectInd
        inNbrs(connectInd) += ind
      }
    }
    // compute the authority&hub using HITS, the authority and hub value for each vertex are initialized as 1.0
    var auth = Array.fill(nRows * nCols)(1.0)
    var hub = Array.fill(nRows * nCols)(1.0)
    for (iter <- 0 until nIter) {
      val oldAuth = auth
      val oldHub = hub
      auth = new Array[Double](nRows * nCols)
      hub = new Array[Double](nRows * nCols)

      //update authority score for each vertex to be the sum of all the Hub scores of pages that point to it
      for (ind <- 0 until (nRows * nCols)) {
        auth(ind) = inNbrs(ind).map( nbr => oldHub(nbr)).sum
      }
      val authTotal = sqrt(auth.map(v => v*v).sum)

      //update hub score for each vertex to be the sum of the Authority scores of all its linking pages
      for (ind <- 0 until (nRows * nCols)) {
	hub(ind) = outNbrs(ind).map( nbr => auth(nbr)).sum
      }
      val hubTotal = sqrt(hub.map(v => v*v).sum)

      auth = auth.map(v => v/authTotal)
      hub = hub.map(v => v/hubTotal)
    }

    var hits = Array.fill(nRows * nCols)(Score(1.0, 1.0))
    for (ind <- 0 until (nRows * nCols)) {
      hits(ind) = Score(auth(ind), hub(ind))
    }
    (0L until (nRows * nCols)).zip(hits)
  }

}


class HITSSuite extends SparkFunSuite with LocalSparkContext {

  def compareRanks(a: VertexRDD[Score], b: VertexRDD[Score]): Double = {
    a.leftJoin(b) { case (id, a, bOpt) => pow(a.authority - bOpt.getOrElse(Score(0.0, 0.0)).authority, 2) + pow(a.hub - bOpt.getOrElse(Score(0.0, 0.0)).hub, 2) }
      .map { case (id, error) => error }.sum()

  }

  test("Star HITS") {
    withSpark { sc =>
      val nVertices = 100
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val errorTol = 1.0e-5

      val staticRanks1 = starGraph.staticHITS(numIter = 1).vertices
      val staticRanks2 = starGraph.staticHITS(numIter = 2).vertices.cache()

      // Static HITS should only take 2 iterations to converge
      val notMatching = staticRanks1.innerZipJoin(staticRanks2) { (vid, score1, score2) =>
        if (score1.authority != score2.authority || score1.hub != score2.hub) 1 else 0
      }.map { case (vid, test) => test }.sum()
      assert(notMatching === 0)

      val refHub = 1.0/nVertices
      val staticErrors = staticRanks2.map { case (vid, score) =>
        val hubDiff = math.abs(score.hub - refHub)
        val correct = (vid > 0 && score.authority == 0.0 && hubDiff < errorTol) || (vid == 0L && score.authority == 1.0 && score.hub == 0.0)
        if (!correct) 1 else 0
      }
      assert(staticErrors.sum === 0)
    }
  } // end of test Star HITS

  test("Grid HITS") {
    withSpark { sc =>
      val rows = 10
      val cols = 10
      val numIter = 50
      val errorTol = 1.0e-5
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols).cache()

      val staticRanks = gridGraph.staticHITS(numIter).vertices.cache()
      val referenceRanks = VertexRDD(
        sc.parallelize(GridHITS(rows, cols, numIter))).cache()

      assert(compareRanks(staticRanks, referenceRanks) < errorTol)
    }
  } // end of Grid HITS


}
