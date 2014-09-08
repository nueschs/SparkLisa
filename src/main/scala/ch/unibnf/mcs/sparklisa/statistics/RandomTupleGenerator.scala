package ch.unibnf.mcs.sparklisa.statistics

import ch.unibnf.mcs.sparklisa.TopologyHelper
import ch.unibnf.mcs.sparklisa.topology.{NodeType, Topology}
import scala.compat.Platform
import scala.util.Random
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.math

/**
 * Created by Stefan Nüesch on 06.09.14.
 */
object RandomTupleGenerator {

  val random : Random = new Random()

  def createRandomNeighboursList(nodeId: String, length: Int, numNodes: Int): List[List[String]] = {
    if (numNodes < 16) {
      return asNodeIds(createRandomPermutationsFromAll(nodeId, length, numNodes))
    } else {
      return asNodeIds(createRandomPermutations(nodeId, length, numNodes))
    }
  }

  private def asNodeIds(permutations: List[List[Int]]): List[List[String]] = {
    return permutations.map(tup => {
      tup.map(x => {
        "node"+x.toString
      })
    })
  }

  private def createRandomPermutations(nodeId: String, length: Int, numNodes: Int): List[List[Int]] = {
    val permutations: mutable.Set[mutable.Set[Int]] = mutable.Set()
    while (permutations.size < length){
      val tup: mutable.Set[Int] = mutable.Set()
      val len = random.nextInt(4)+1

      while (tup.size < len) {
        var rand = -1
        while (rand < 0 || rand == nodeId.takeRight(1).toInt) {
          rand = random.nextInt(numNodes)
        }
        tup += rand
      }

      permutations += tup
    }

    return permutations.map(x => x.toList).toList
  }

  private def createRandomPermutationsFromAll(nodeId: String, length: Int, numNodes: Int): List[List[Int]] = {
    val allPermutations: mutable.MutableList[List[Int]] = mutable.MutableList()
    for (i <- 1 to 4){
      allPermutations ++= createAllPermutations(numNodes, i)
    }
    return random.shuffle(allPermutations).take(length).toList.filter(p => p.toString() != nodeId.takeRight(1))
  }

  private def createAllPermutations(n: Int, k: Int): List[List[Int]] = {
    return (0 to n).toList.combinations(k).toList
  }
}
