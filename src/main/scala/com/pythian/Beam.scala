package com.pythian

import java.io.FileInputStream
import com.typesafe.scalalogging.LazyLogging
import com.google.api.services.bigquery.model.{ TableReference, TableRow }
import com.google.api.services.dataflow.DataflowScopes
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.gson.JsonParser
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.values.PCollectionView
//import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.options.{ PipelineOptions, PipelineOptionsFactory }
import org.apache.beam.sdk.transforms.{ DoFn, ParDo }
import DoFn.ProcessElement
import scala.util.{ Failure, Success, Try }
import Dynamic._

object Beam extends App {

  /* location of service account key */
  val keyLocation = "key.json"

  /* parameters for pipeline */
  val projectId = "data-enablement-platfrom"
  val gcpTempLocation = "gs://beam-template/temp"
  val dataset = "test_nikotin"
  val tableName = "test"
  val subscription = "test-dataflow"
  val sideInputSubscription = "test-dataflow-sideinput"

  /* configuration for runner */
  //val runner = classOf[DataflowRunner]
  val runner = classOf[DirectRunner]
  val numWorkers = 4
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

  /* pipeline reading messages from given subscription and streaming them into BigQuery table,
     using sideInput as source code of json to TableRow converter */
  val fullSubscriptionName = s"projects/$projectId/subscriptions/$subscription"
  val fullSideInputSubscriptionName = s"projects/$projectId/subscriptions/$sideInputSubscription"
  val targetTable = new TableReference().setProjectId(projectId).setDatasetId(dataset).setTableId(tableName)

  /* converting strings to table row, getting dynamic parsed memoized mapping functions */
  class MyDoFn(sideView: PCollectionView[java.util.List[String]]) extends DoFn[String, TableRow] with LazyLogging {
    @ProcessElement
    def processElement(c: ProcessContext) {
      val t0 = System.currentTimeMillis()
      val sideInput = c.sideInput(sideView).get(0)
      val inputString = c.element()
      Try {
        val json = new JsonParser().parse(inputString).getAsJsonObject
        sideInput.evalFor(json)
      } match {
        case Success(row) ⇒
          logger.info(s"Inserting to BiqQuery: $row")
          c.output(row)
        case Failure(ex) ⇒
          logger.info(s"Unable to parse message: $inputString", ex)
      }
      val t1 = System.currentTimeMillis()
      logger.info(s"Processed data in ${t1 - t0} ms")
    }
  }

  /* building pipeline */
  val p = Pipeline.create(options)

  /* reading from pubsub with controls, applying global window and triggering for every incoming message */
  val sideView = p.apply("read-pubsub-side", PubsubIO.readStrings().fromSubscription(fullSideInputSubscriptionName))
    .apply(
      "global_side_input",
      Window.into[String](new GlobalWindows())
        .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
        .discardingFiredPanes())
    .apply("side_view", View.asList())

  /* final pipeline */
  p.apply("read-pubsub", PubsubIO.readStrings().fromSubscription(fullSubscriptionName))
    .apply("process", ParDo.of(new MyDoFn(sideView)).withSideInputs(sideView))
    .apply("write-bq", BigQueryIO
      .writeTableRows()
      .to(targetTable)
      .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER))

  /* start pipeline */
  p.run()

  /* Examples of control messages:

  (json: com.google.gson.JsonObject) => {
    new com.google.api.services.bigquery.model.TableRow()
      .set("id", json.get("id").getAsLong)
      .set("data", json.get("text").getAsString)
  }

  (json: com.google.gson.JsonObject) => {
    new com.google.api.services.bigquery.model.TableRow()
      .set("id", json.get("id").getAsLong)
      .set("data", "Dummy placeholder")
  }
  */

}