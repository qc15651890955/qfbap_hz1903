package qfbap.spark

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import qfbap.conf.ConfigManager
import qfbap.constant.constants
import qfbap.utils.JDBCUtils

object DM_USER_VISIT {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(constants.SPARK_APP_NAME_USER).master(constants.SPARK_LOCAL).enableHiveSupport().getOrCreate()
    val sql = ConfigManager.getProper(args(0))
    if(sql == null) {
      LoggerFactory.getLogger("SparkLogger")
        .debug("提交的表名参数有问题，请重新设置！！！！")
    } else {
      val df = spark.sql(sql)
      //df.show()
      val mysqlTableName = args(0).split("\\.")(1)
      val hiveTableName = args(0)
      val jdbcProp = JDBCUtils.getJdbcProp()._1
      val jdbcUrl = JDBCUtils.getJdbcProp()._2
      //df.write.mode(SaveMode.Append).jdbc(jdbcUrl,mysqlTableName,jdbcProp)
      df.write.saveAsTable(hiveTableName)
    }
  }

}
