package ch.unibnf.mcs.sparklisa.receiver

import scala.io.Source

object MyTest {
    def main(a: Array[String]){
      var text = Source.fromInputStream(getClass().getResourceAsStream("/node_values_4.txt")).mkString
      var textArr = text.split("\n")
      var newArr = textArr.map { l =>
        l.split(";")(0)
      }
      newArr.foreach { value =>
        print(value+"\n")
      }
    }
}
