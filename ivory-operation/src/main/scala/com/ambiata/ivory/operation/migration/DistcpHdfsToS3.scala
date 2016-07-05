package com.ambiata.ivory.operation.migration

/*import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage._*/
import java.net.InetAddress

import sys.process._

object DistcpHdfsToS3 {
  def copyToS3(src:String,dest:String):String = {
    val source = src.replace("hdfs://", "hdfs://"+InetAddress.getLocalHost().getHostAddress)
    val destination:String = dest
    //Command to run distcp
    s"hadoop distcp -update $source $destination".!!
  }
}