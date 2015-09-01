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
import org.apache.spark.graphx.util.GraphGenerators


object GridHITS {
  def apply(nRows: Int, nCols: Int, nIter: Int): Seq[(VertexId, (Double, Double))] = {
    
    /*val inNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])
    val outDegree = Array.fill(nRows * nCols)(0)
    // Convert row column address into vertex ids (row major order)
    def sub2ind(r: Int, c: Int): Int = r * nCols + c
    // Make the grid graph
    for (r <- 0 until nRows; c <- 0 until nCols) {
      val ind = sub2ind(r, c)
      if (r + 1 < nRows) {
        outDegree(ind) += 1
        inNbrs(sub2ind(r + 1, c)) += ind
      }
      if (c + 1 < nCols) {
        outDegree(ind) += 1
        inNbrs(sub2ind(r, c + 1)) += ind
      }
    }
    // compute the pagerank
    var pr = Array.fill(nRows * nCols)(resetProb)
    for (iter <- 0 until nIter) {
      val oldPr = pr
      pr = new Array[Double](nRows * nCols)
      for (ind <- 0 until (nRows * nCols)) {
        pr(ind) = resetProb + (1.0 - resetProb) *
          inNbrs(ind).map( nbr => oldPr(nbr) / outDegree(nbr)).sum
      }
    }*/

    // to do
    var hits = Array.fill(nRos * nCols)((1.0, 1.0))
    (0L until (nRows * nCols)).zip(hits)
  }

}


class HITSSuite extends SparkFunSuite with LocalSparkContext {

  def compareRanks(a: VertexRDD[Double], b: VertexRDD[Double]): Double = {
    //a.leftJoin(b) { case (id, a, bOpt) => (a - bOpt.getOrElse(0.0)) * (a - bOpt.getOrElse(0.0)) }
      //.map { case (id, error) => error }.sum()

    //to do
    return 0.0 
  }

  test("Star HITS") {
    withSpark { sc =>
      val nVertices = 100
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val errorTol = 1.0e-5

      /*val staticRanks1 = starGraph.staticHITS(numIter = 1).vertices
      val staticRanks2 = starGraph.staticHITS(numIter = 2).vertices.cache()

      // Static HITS should only take 2 iterations to converge
      val notMatching = staticRanks1.innerZipJoin(staticRanks2) { (vid, pr1, pr2) =>
        if (pr1 != pr2) 1 else 0
      }.map { case (vid, test) => test }.sum()
      assert(notMatching === 0)

      val staticErrors = staticRanks2.map { case (vid, pr) =>
        val p = math.abs(pr - (resetProb + (1.0 - resetProb) * (resetProb * (nVertices - 1)) ))
        val correct = (vid > 0 && pr == resetProb) || (vid == 0L && p < 1.0E-5)
        if (!correct) 1 else 0
      }
      assert(staticErrors.sum === 0)*/

    }
  } // end of test Star HITS

  test("Grid HITS") {
    withSpark { sc =>
      val rows = 10
      val cols = 10
      val tol = 0.0001
      val numIter = 50
      val errorTol = 1.0e-5
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols).cache()

      /*val staticRanks = gridGraph.staticHITS(numIter, resetProb).vertices.cache()
      val referenceRanks = VertexRDD(
        sc.parallelize(GridHITS(rows, cols, numIter, resetProb))).cache()

      assert(compareRanks(staticRanks, referenceRanks) < errorTol)*/
    }
  } // end of Grid HITS

  test("Chain HITS") {
    withSpark { sc =>
      val chain1 = (0 until 9).map(x => (x, x + 1))
      val rawEdges = sc.parallelize(chain1, 1).map { case (s, d) => (s.toLong, d.toLong) }
      val chain = Graph.fromEdgeTuples(rawEdges, 1.0).cache()
      val tol = 0.0001
      val numIter = 10
      val errorTol = 1.0e-5

      /*val staticRanks = chain.staticHITS(numIter, resetProb).vertices

      assert(compareRanks(staticRanks, dynamicRanks) < errorTol)*/
    }
  }

}
