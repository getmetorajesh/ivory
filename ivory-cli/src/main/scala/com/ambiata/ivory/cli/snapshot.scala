package com.ambiata.ivory.cli

import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.cli.extract._
import com.ambiata.ivory.cli.PirateReaders._
import com.ambiata.ivory.core._
import com.ambiata.ivory.api.Ivory.{Date => _, _}
import com.ambiata.ivory.operation.extraction.squash.SquashJob
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.operation.migration._
import com.ambiata.mundane.control.RIO

import java.util.UUID

import org.joda.time.LocalDate

import pirate._, Pirate._

import scalaz._, Scalaz._

object snapshot extends IvoryApp {

  val cmd = Command(
    "snapshot"
    , Some("""
             |Take a snapshot of facts from an ivory repo
             |
             |This will extract the latest facts for every entity relative to a date (default is now)
             |
             |""".stripMargin)

    , ( flag[Date](both('d', "date"), description("Optional date to take snapshot from, default is now."))
      .default(Date.fromLocalDate(LocalDate.now))
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

      )((date, squash, formats, clusterLoad, loadRepo,extLoc) =>
      IvoryRunner(configuration => loadRepo(configuration).flatMap(repoAndFlags => {

        val repo = repoAndFlags._1
        val runId = UUID.randomUUID
        val banner = s"""======================= snapshot =======================
                         |
                      |Arguments --
                         |
                      |  Run ID                  : ${runId}
                         |  Ivory Repository        : ${repo.root.show}
                         |  Extract At Date         : ${date.slashed}
                         |  Outputs                 : ${formats.formats.mkString(", ")}
                         |
                      |""".stripMargin
        println(banner)
        IvoryT.fromRIO { for {
          of       <- Extract.parse(configuration, formats)
          econf    <- ExtractionConfig.fromProperties
          snapshot <- IvoryRetire.takeSnapshot(repo, repoAndFlags._2, date)
          _        <- RIO.when(of.outputs.nonEmpty, {
            val cluster = clusterLoad(configuration)
            for {
              x <- SquashJob.squashFromSnapshotWith(repo, snapshot, squash, cluster)
              (output, dictionary) = x
              r <- RepositoryRead.fromRepository(repo)
              _ <- Extraction.extract(of, output, dictionary, cluster, econf).run(r)
            } yield ()
          })
        } yield List(banner, s"Snapshot complete: ${snapshot.id}.'${DistcpHdfsToS3.copyToExternal(repo.root.location.render+"/snapshots",extLoc)}'") }
      }))))
}