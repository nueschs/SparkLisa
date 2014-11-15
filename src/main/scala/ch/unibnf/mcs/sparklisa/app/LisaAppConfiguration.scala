package ch.unibnf.mcs.sparklisa.app

import java.util.Properties

import org.apache.spark.SparkConf

/**
 * Reusable Constants and config for all applications
 */
trait LisaAppConfiguration {

  val SumKey: String = "SUM_KEY"
  val config: Properties = new Properties()
  var Env: String = null
  var HdfsPath: String = null
  var Strategy = None: Option[String]
  val Master: String

  /**
   * Load properties file from classpath. Env is set during maven build - be sure to use
   * -Pcluster flag when building for cluster
   */
  def initConfig() = {
    config.load(getClass.getClassLoader.getResourceAsStream("config.properties"))
    Env = config.getProperty("build.env")
    HdfsPath = config.getProperty("hdfs.path." + Env)
    Strategy = Some(config.getProperty("receiver.strategy"))
  }

  /**
   * Master does only have to be set when running locally. On the cluster, this is done via spark-submit
   * @return
   */
  def createSparkConf(): SparkConf = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("File Input LISA Streaming Job")
    if ("local" == Env) {
      conf.setMaster(Master)
        .setSparkHome("/home/snoooze/spark/spark-1.0.0")
        .setJars(Array[String]("target/SparkLisa-0.0.1-SNAPSHOT.jar"))
    }

    return conf
  }

}
