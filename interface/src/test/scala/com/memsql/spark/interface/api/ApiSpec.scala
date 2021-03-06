package com.memsql.spark.interface.api

import java.util.UUID

import akka.actor.Props
import com.memsql.spark.etl.api.{UserTransformConfig, UserExtractConfig}
import com.memsql.spark.etl.api.configs._
import com.memsql.spark.interface._
import ApiActor._
import com.memsql.spark.phases.{ZookeeperManagedKafkaExtractConfig, JsonTransformConfig}
import com.memsql.spark.phases.configs.ExtractPhase
import scala.concurrent.duration._
import spray.json._

import scala.util.{Success, Failure}

import ExtractPhaseKind._
import TransformPhaseKind._
import LoadPhaseKind._

// scalastyle:off magic.number
class ApiSpec extends TestKitSpec("ApiActorSpec") {
  var mockTime = new MockTime()
  val apiRef = system.actorOf(Props(classOf[TestApiActor], mockTime))

  "Api actor" should {
    val config = PipelineConfig(
      Phase[ExtractPhaseKind](
        ExtractPhaseKind.ZookeeperManagedKafka,
        ExtractPhase.writeConfig(
          ExtractPhaseKind.ZookeeperManagedKafka, ZookeeperManagedKafkaExtractConfig(List("test1:2181"), "topic"))),
      Phase[TransformPhaseKind](
        TransformPhaseKind.Json,
        TransformPhase.writeConfig(
          TransformPhaseKind.Json, JsonTransformConfig("data"))),
      Phase[LoadPhaseKind](
        LoadPhaseKind.MemSQL,
        LoadPhase.writeConfig(
          LoadPhaseKind.MemSQL, MemSQLLoadConfig("db", "table", None, None))))

    val config2 = config.copy(extract = Phase[ExtractPhaseKind](
      ExtractPhaseKind.User,
      ExtractPhase.writeConfig(
        ExtractPhaseKind.User, UserExtractConfig("com.memsql.spark.interface.support.DummyExtractor", JsString("test")))))

    val nonExistingExtractConfig = config.copy(extract = Phase[ExtractPhaseKind](
      ExtractPhaseKind.User,
      ExtractPhase.writeConfig(
        ExtractPhaseKind.User, UserExtractConfig("com.memsql.spark.interface.support.DumDumDum", JsString("test")))))

    val nonExistingTransformConfig = config.copy(transform = Phase[TransformPhaseKind](
      TransformPhaseKind.User,
      TransformPhase.writeConfig(
        TransformPhaseKind.User, UserTransformConfig("com.memsql.spark.interface.support.DumDumDum", JsString("test")))))

    "respond to ping" in {
      apiRef ! Ping
      expectMsg("pong")
    }

    "have no pipelines to start" in {
      apiRef ! PipelineQuery
      expectMsg(List())

      apiRef ! PipelineGet("pipelinenotthere")
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      apiRef ! PipelineUpdate("pipelinenotthere", Some(PipelineState.STOPPED))
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }
    }

    "fail with a non-existing class" in {
      apiRef ! PipelinePut("pipeline1", batch_interval=10, config=nonExistingExtractConfig)
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      apiRef ! PipelinePut("pipeline1", batch_interval=10, config=nonExistingTransformConfig)
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }
    }

    "accept new pipelines" in {
      apiRef ! PipelinePut("pipeline1", batch_interval=10, config=config)
      expectMsg(Success(true))

      // update time for new pipeline should be greater than this
      // error if pipeline id already exists
      apiRef ! PipelinePut("pipeline1", batch_interval=10, config=config)
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      apiRef ! PipelineQuery
      receiveOne(1.second) match {
        case pipelines:List[_] => assert(pipelines.length == 1)
        case default => fail(s"unexpected response $default")
      }

      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp:Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.pipeline_id == "pipeline1")
          assert(pipeline.state == PipelineState.RUNNING)
          assert(pipeline.batch_interval == 10)
          assert(pipeline.config == config)
          assert(pipeline.last_updated == 0)
          val kafkaConfig = ExtractPhase.readConfig(pipeline.config.extract.kind, pipeline.config.extract.config)
                            .asInstanceOf[ZookeeperManagedKafkaExtractConfig]
          assert(kafkaConfig.zk_quorum == List("test1:2181"))
          assert(kafkaConfig.topic == "topic")
        case Failure(err) => fail(s"unexpected response $err")
      }

      mockTime.tick
      apiRef ! PipelinePut("pipeline2", batch_interval=10, config=config2)
      expectMsg(Success(true))

      // error if config is invalid
      val badConfig = config.copy(extract = Phase[ExtractPhaseKind](ExtractPhaseKind.ZookeeperManagedKafka,
        """{ "bad_kafka_config": 42 }""".parseJson))
      apiRef ! PipelinePut("pipeline3", batch_interval=10, config=badConfig)
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      // error if batch_interval is invalid
      apiRef ! PipelinePut("pipeline3", batch_interval= -10, config=config)
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      apiRef ! PipelineQuery
      receiveOne(1.second) match {
        case pipelines:List[_] => assert(pipelines.length == 2)
        case default => fail(s"unexpected response $default")
      }

      apiRef ! PipelineGet("pipeline2")
      receiveOne(1.second) match {
        case resp:Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.pipeline_id == "pipeline2")
          assert(pipeline.state == PipelineState.RUNNING)
          assert(pipeline.config == config2)
          assert(pipeline.last_updated == 1)
          val userConfig = ExtractPhase.readConfig(pipeline.config.extract.kind, pipeline.config.extract.config).asInstanceOf[UserExtractConfig]
          assert(userConfig.value.toString == "\"test\"")
        case Failure(err) => fail(s"unexpected response $err")
      }
    }

    "fail with a non-existing class for update" in {
      apiRef ! PipelineUpdate("pipeline1", config=Some(nonExistingExtractConfig))
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      apiRef ! PipelineUpdate("pipeline1", config=Some(nonExistingTransformConfig))
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }
    }

    "allow updates to pipelines" in {
      mockTime.tick
      apiRef ! PipelineUpdate("pipeline1", state=Some(PipelineState.STOPPED))
      expectMsg(Success(true))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.state == PipelineState.STOPPED)
          assert(pipeline.last_updated == 2)
        case Failure(err) => fail(s"unexpected response $err")
      }

      //no-op updates return false and update time should not be changed
      mockTime.tick
      apiRef ! PipelineUpdate("pipeline1", state=Some(PipelineState.STOPPED))
      expectMsg(Success(false))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.state == PipelineState.STOPPED)
          assert(pipeline.last_updated == 2)
        case Failure(err) => fail(s"unexpected response $err")
      }

      // All parameters are optional for PipelineUpdate except pipeline_id
      apiRef ! PipelineUpdate("pipeline1")
      expectMsg(Success(false))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.state == PipelineState.STOPPED)
          assert(pipeline.last_updated == 2)
        case Failure(err) => fail(s"unexpected response $err")
      }

      mockTime.tick
      apiRef ! PipelineUpdate("pipeline1", state=Some(PipelineState.ERROR), error=Some("something crashed"))
      expectMsg(Success(true))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.state == PipelineState.ERROR)
          assert(pipeline.error.get == "something crashed")
          assert(pipeline.last_updated == 4)
        case Failure(err) => fail(s"unexpected response $err")
      }

      // updates to batch interval should only be accepted if interval is positive and non-zero
      mockTime.tick
      apiRef ! PipelineUpdate("pipeline1", batch_interval = Some(1234))
      expectMsg(Success(true))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.batch_interval == 1234)
          assert(pipeline.last_updated == 5)
        case Failure(err) => fail(s"unexpected response $err")
      }

      // updates should be transactional
      mockTime.tick
      apiRef ! PipelineUpdate("pipeline1", batch_interval = Some(0), config = Some(config2))
      receiveOne(1.second) match {
        case resp: Success[_] => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.batch_interval == 1234)
          assert(pipeline.config == config)
          assert(pipeline.last_updated == 5)
        case Failure(err) => fail(s"unexpected response $err")
      }

      //an update request from the api must be validated and cannot perform all updates
      mockTime.tick
      apiRef ! PipelineUpdate("pipeline1", state=Some(PipelineState.STOPPED), _validate=true)
      receiveOne(1.second) match {
        case resp: Success[_] => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.state == PipelineState.ERROR)
          assert(pipeline.error.get == "something crashed")
          assert(pipeline.last_updated == 5)
        case Failure(err) => fail(s"unexpected response $err")
      }

      // Updating configs should be allowed
      val newConfig = PipelineConfig(
        Phase[ExtractPhaseKind](
          ExtractPhaseKind.ZookeeperManagedKafka,
          ExtractPhase.writeConfig(
            ExtractPhaseKind.ZookeeperManagedKafka, ZookeeperManagedKafkaExtractConfig(List("test2:2181/chroot", "test1:2182"), "test2"))),
        Phase[TransformPhaseKind](
          TransformPhaseKind.User,
          TransformPhase.writeConfig(
            TransformPhaseKind.User, UserTransformConfig("com.memsql.spark.interface.support.DummyTransformer", JsString("test1")))),
        Phase[LoadPhaseKind](
          LoadPhaseKind.MemSQL,
          LoadPhase.writeConfig(
            LoadPhaseKind.MemSQL, MemSQLLoadConfig("db", "table", None, None))))

      mockTime.tick
      apiRef ! PipelineUpdate("pipeline1", config=Some(newConfig))
      expectMsg(Success(true))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.config == newConfig)
          assert(pipeline.config.extract.kind == ExtractPhaseKind.ZookeeperManagedKafka)
          val kafkaConfig = ExtractPhase.readConfig(pipeline.config.extract.kind, pipeline.config.extract.config)
                            .asInstanceOf[ZookeeperManagedKafkaExtractConfig]
          assert(kafkaConfig.zk_quorum == List("test2:2181/chroot", "test1:2182"))
          assert(kafkaConfig.topic == "test2")
          assert(pipeline.last_updated == 8)
        case Failure(err) => fail(s"unexpected response $err")
      }

      // Configs that do not deserialize should be rejected.
      val badConfig = newConfig.copy(extract = Phase[ExtractPhaseKind](
          ExtractPhaseKind.Kafka, """{ "bad_kafka_config": 42 }""".parseJson))
      mockTime.tick
      apiRef ! PipelineUpdate("pipeline1", config=Some(badConfig))
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.config == newConfig)
          assert(pipeline.last_updated == 8)
        case Failure(err) => fail(s"unexpected response $err")
      }

      // Updating config with the same value is a no-op
      mockTime.tick
      apiRef ! PipelineUpdate("pipeline1", config=Some(newConfig))
      expectMsg(Success(false))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.last_updated == 8)
          assert(pipeline.config == newConfig)
        case Failure(err) => fail(s"unexpected response $err")
      }

      // Updating just the trace batch count should not change last_updated
      // but it should still return true
      mockTime.tick
      apiRef ! PipelineUpdate("pipeline1", trace_batch_count=Some(10))
      expectMsg(Success(true))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.last_updated == 8)
          assert(pipeline.traceBatchCount == 10)
        case Failure(err) => fail(s"unexpected response $err")
      }

      // No-op updates to trace batch count should behave the same as other no-op updates
      mockTime.tick
      apiRef ! PipelineUpdate("pipeline1", trace_batch_count=Some(10))
      expectMsg(Success(false))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.last_updated == 8)
          assert(pipeline.traceBatchCount == 10)
        case Failure(err) => fail(s"unexpected response $err")
      }

      mockTime.tick
      apiRef ! PipelineUpdate("pipeline1", threadState=Some(PipelineThreadState.THREAD_RUNNING))
      expectMsg(Success(true))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.thread_state == PipelineThreadState.THREAD_RUNNING)
        case Failure(err) => fail(s"unexpected response $err")
      }

      mockTime.tick
      apiRef ! PipelineUpdate("pipeline1", trace_batch_count=Some(3), threadState=Some(PipelineThreadState.THREAD_STOPPED))
      expectMsg(Success(true))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.thread_state == PipelineThreadState.THREAD_STOPPED)
          assert(pipeline.traceBatchCount == 3)
        case Failure(err) => fail(s"unexpected response $err")
      }

      mockTime.tick
      apiRef ! PipelineUpdate("pipeline1", batch_interval = Some(2), threadState=Some(PipelineThreadState.THREAD_RUNNING))
      expectMsg(Success(true))
      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          assert(pipeline.thread_state == PipelineThreadState.THREAD_RUNNING)
          assert(pipeline.traceBatchCount == 3)
          assert(pipeline.batch_interval == 2)
        case Failure(err) => fail(s"unexpected response $err")
      }
    }

    "return metrics when available" in {
      apiRef ! PipelineMetrics("pipeline1", None)
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val l = resp.get.asInstanceOf[List[BatchEndEvent]]
          assert(l.size == 0)
        case Failure(err) => fail(s"unexpected response $err")
      }

      val record1 = BatchEndEvent(batch_id = "batch1", batch_type = PipelineBatchType.Normal, pipeline_id = "pipeline1",
                                         timestamp = 100, success = true, task_errors = None, extract = None, transform = None,
                                         load = None, event_id = UUID.randomUUID.toString)
      val record2 = BatchEndEvent(batch_id = "batch2", batch_type = PipelineBatchType.Normal, pipeline_id = "pipeline1",
                                         timestamp = 110, success = false, task_errors = None, extract = None, transform = None,
                                         load = None, event_id = UUID.randomUUID.toString)

      apiRef ! PipelineGet("pipeline1")
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val pipeline = resp.get.asInstanceOf[Pipeline]
          // Add some fake metrics.
          pipeline.enqueueMetricRecord(record1)
          pipeline.enqueueMetricRecord(record2)
        case Failure(err) => fail(s"unexpected response $err")
      }

      apiRef ! PipelineMetrics("pipeline1", None)
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val l = resp.get.asInstanceOf[List[BatchEndEvent]]
          assert(l == List(record1, record2))
        case Failure(err) => fail(s"unexpected response $err")
      }

      apiRef ! PipelineMetrics("pipeline1", last_timestamp = Some(105))
      receiveOne(1.second) match {
        case resp: Success[_] =>
          val l = resp.get.asInstanceOf[List[BatchEndEvent]]
          assert(l == List(record2))
        case Failure(err) => fail(s"unexpected response $err")
      }
    }

    "allow deleting pipelines" in {
      apiRef ! PipelineDelete("pipeline2")
      expectMsg(Success(true))

      apiRef ! PipelineQuery
      receiveOne(1.second) match {
        case pipelines:List[_] => assert(pipelines.length == 1)
        case default => fail(s"unexpected response $default")
      }

      apiRef ! PipelineGet("pipeline2")
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }

      apiRef ! PipelineDelete("pipeline2")
      receiveOne(1.second) match {
        case Success(resp) => fail(s"unexpected response $resp")
        case Failure(err) => assert(err.isInstanceOf[ApiException])
      }
    }
  }
}
