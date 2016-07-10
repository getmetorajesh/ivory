package com.ambiata.ivory.operation.migration

import java.net.InetAddress
import sys.process._
/**
 * Author: Kirupa Devarajan
 * Transfers ingest output(factsets) to user supplied S3 location
 * if user supplies NA value, no transfer will be made
 */
object DistcpHdfsToS3 {
  def copyToExternal(src:String,dest:String):String= {
    val source = src.replace("hdfs://", "hdfs://"+InetAddress.getLocalHost().getHostAddress)
    val destination:String = dest
    if(destination.toLowerCase().equals("na")){
      "\n\nINFO: Output not copied to any External Location\n\n"
    }
    else{
      s"\n\nStarting to copy data from HDFS to $destination\n\n"
      //Command to run distcp
      s"hadoop distcp -update $source $destination".!!
      s"\n\nSuccessfully copied output to $destination\n"
    }
  }
}