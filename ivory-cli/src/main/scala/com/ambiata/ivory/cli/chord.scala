package com.ambiata.ivory.cli

import com.ambiata.ivory.api.Ivory._
import com.ambiata.ivory.cli.extract._
import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.extraction.Chord
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control._
import com.ambiata.ivory.operation.migration._

import pirate._, Pirate._

import scalaz._, Scalaz._

object chord extends IvoryApp {

  val cmd = Command(
    "chord"
    , Some("""
             |Extract the latest features from a given ivory repo using a list of entity id and date pairs
             |
             |The output entity ids will be of the form eid:yyyy-MM-dd
             |""".stripMargin)

    , ( flag[String](both('c', "entities"), description("Path to file containing entity/date pairs (eid|yyyy-MM-dd)."))
      |@| switch(long("no-snapshot"), description("Do not take a new snapshot, just any existing.")).map(!_)
      |@| Extract.parseSquashConfig
      |@| Extract.parseOutput
      |@| IvoryCmd.cluster
      |@| IvoryCmd.repositoryWithFlags
      //Added by Kirupa to support moving of command output to external location
      |@| flag[String](both('e', "copyto"), description("""
                                                          |External location where the output should be copied (S3 or hdfs), eg. --copyto "s3://bucketname/folder/"
                                                          |
                                                          |Number of maps [OPTIONAL] to run the copy job can be specified as --copyto "s3://bucketname/folder/ 10"
                                                          |
                                                          |Use NA as value for --copyto parameter if the data need not be copied to external location.eg. --copyto "NA"
                                                          |""".stripMargin))

      )((entities, takeSnapshot, squash, formats, loadCluster, loadRepo,extLoc) =>
      IvoryRunner(conf => loadRepo(conf).flatMap(repoAndFlags =>

        IvoryT.fromRIO { for {
          ent  <- IvoryLocation.fromUri(entities, conf)
          repo  = repoAndFlags._1
          of   <- Extract.parse(conf, formats)
          ec   <- ExtractionConfig.fromProperties
          _    <- RIO.when(of.outputs.isEmpty, RIO.fail[Unit]("No output/format specified"))
          // The problem is that we should be outputting the output with a separate date - currently it's hacked into the entity
          _    <- RIO.when(of.outputs.exists(_._1.format.isThrift), RIO.fail[Unit]("Thrift output for chord not currently supported"))
          r    <- RepositoryRead.fromRepository(repo)
          cluster = loadCluster(conf)
          // TODO Should be using Ivory API here, but the generic return type is lost on the monomorphic function
          x    <- Chord.createChordWithSquash(repo, repoAndFlags._2, ent, takeSnapshot, squash, cluster)
          (out, dict) = x
          _    <- Extraction.extract(of, out, dict, cluster, ec).run(r)
        } yield List(s"Successfully extracted chord from '${repo.root.show}'.'${DistcpHdfsToS3.copyToExternal(repo.root.location.render+"/snapshots",extLoc)}'") }
      ))))
}