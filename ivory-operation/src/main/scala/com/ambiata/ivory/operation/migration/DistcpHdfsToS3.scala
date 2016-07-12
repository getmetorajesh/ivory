package com.ambiata.ivory.operation.migration

import java.net.InetAddress
import sys.process._
/**
  * Author: Kirupa Devarajan
  * Transfers ingest output(factsets) to user supplied S3 location
  * If user supplies NA value, no transfer will be made
  * Optionally users can supply map parameter to the distcp job
  */
object DistcpHdfsToS3 {
  def copyToExternal(src: String, dest: String): String = {
    val source = src.replace("hdfs://", "hdfs://" + InetAddress.getLocalHost().getHostAddress)

    //create parameters for distcp job
    val param = dest.split(" ") match {
      case Array(dest, maps) => "-update -m " + maps
      case Array(dest)       => s"-update"
    }

    //create destination location
    val destination: String = dest.split(" ")(0)
    if (destination.toLowerCase().equals("na")) {
      "\n\nINFO: Output not copied to any External Location\n\n"
    } else {
      s"\n\nStarting to copy data from HDFS to $destination\n\n"
      //Command to run distcp
      s"hadoop distcp $param $source $destination".!!
      s"\n\nSuccessfully copied output to $destination\n"
    }
  }
}