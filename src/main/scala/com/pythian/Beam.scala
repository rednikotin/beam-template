package com.pythian

import java.io.FileInputStream
import com.typesafe.scalalogging.LazyLogging
import com.google.api.services.bigquery.model.{ TableReference, TableRow }
import com.google.api.services.dataflow.DataflowScopes
import com.google.auth.oauth2.ServiceAccountCredentials
//import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.options.{ PipelineOptions, PipelineOptionsFactory }
import org.apache.beam.sdk.transforms.{ DoFn, ParDo }
import DoFn.ProcessElement
import org.apache.beam.runners.direct.DirectRunner
//import org.apache.beam.sdk.coders.{ AvroCoder, DefaultCoder }
import org.apache.beam.sdk.util.Transport
import scala.util.{ Failure, Success, Try }

object Beam extends App {

  /* location of service account key */
  val keyLocation = "key.json"

  /* parameters for pipeline */
  val projectId = "data-enablement-platfrom"
  val gcpTempLocation = "gs://beam-template/temp"
  val dataset = "test_nikotin"
  val tableName = "test"
  val subscription = "test-dataflow"

  /* configuration for runner */
  //val runner = classOf[DataflowRunner]
  val runner = classOf[DirectRunner]
  val numWorkers = 1
  val zone = "us-west1-a"
  val workerMachineType = "n1-standard-1"

  /* create option object to start pipeline */
  trait TestOptions extends PipelineOptions with DataflowPipelineOptions
  val options = PipelineOptionsFactory.create().as(classOf[TestOptions])

  options.setGcpCredential(ServiceAccountCredentials.fromStream(new FileInputStream(keyLocation)).createScoped(DataflowScopes.all))
  options.setProject(projectId)
  options.setRunner(runner)
  options.setNumWorkers(numWorkers)
  options.setZone(zone)
  options.setWorkerMachineType(workerMachineType)
  options.setGcpTempLocation(gcpTempLocation)
  options.setStreaming(true)

  /* pipeline reading messages from given subscription and streaming them into BigQuery table */
  val fullSubscriptionName = s"projects/$projectId/subscriptions/$subscription"
  val targetTable = new TableReference().setProjectId(projectId).setDatasetId(dataset).setTableId(tableName)

  /* check more information about encoding https://beam.apache.org/documentation/programming-guide/#data-encoding-and-type-safety */
  //@DefaultCoder(classOf[AvroCoder[TableRow]])

  /* Simple DoFn: trying to convert Strings into TableRow */
  class MyDoFn extends DoFn[String, TableRow] with LazyLogging {
    @ProcessElement
    def processElement(c: ProcessContext) {
      val inputString = c.element()
      logger.info(s"Received message: $inputString")
      Try {
        Transport.getJsonFactory.fromString(inputString, classOf[TableRow])
      } match {
        case Success(row) ⇒
          logger.info(s"Converted to TableRow: $row")
          c.output(row)
        case Failure(ex) ⇒
          logger.info(s"Unable to parse message: $inputString", ex)
      }
    }
  }

  /* building pipeline to read Strings from PubsubIO, convert them to TableRows and write to BQ table, not validating table schema */
  val p = Pipeline.create(options)
  p.apply("read-pubsub", PubsubIO.readStrings().fromSubscription(fullSubscriptionName))
    .apply("process", ParDo.of(new MyDoFn))
    .apply("write-bq", BigQueryIO
      .writeTableRows()
      .to(targetTable)
      .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER))

  /* start pipeline */
  p.run()

}