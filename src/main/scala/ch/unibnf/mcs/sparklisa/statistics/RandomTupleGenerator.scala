package ch.unibnf.mcs.sparklisa.statistics

import scala.collection.mutable
import scala.util.Random

object RandomTupleGenerator {
  val random : Random = new Random()

  def main(args: Array[String]) = {
    println(createRandomNeighboursList("node1", 100, 4))
  }

  def createRandomNeighboursList(nodeId: String, length: Int, numNodes: Int): List[List[String]] = {
    if (numNodes < 16) {
      return asNodeIds(createRandomPermutationsFromAll(nodeId, length, numNodes))
    } else {
      return asNodeIds(createRandomPermutations(nodeId, length, numNodes))
    }
  }

  def createRandomNeighboursListNumerical(nodeId: String, length: Int, numNodes: Int): List[List[Int]] = {
    val nodeIds: List[List[String]] = createRandomNeighboursList(nodeId, length, numNodes)
    for (x <- nodeIds) yield for (y <- x) yield y.substring(4).toInt
  }

  private def asNodeIds(permutations: List[List[Int]]): List[List[String]] = {
    return permutations.map(tup => {
      tup.map(x => {
        "node"+(x+1).toString
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
        while (rand < 0 || rand+1 == nodeId.substring(4).toInt) {
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
    return (0 to n-1).toList.combinations(k).toList
  }
}
