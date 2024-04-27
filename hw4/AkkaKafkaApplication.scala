package hw4.kafka.otus;

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.{ActorMaterializer, ClosedShape, UniformFanInShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source, ZipWith}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}

import java.time.InstantSource.system
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

object AkkaKafkaApplication extends App {
  implicit val actorSystem = ActorSystem("AkkaStream")
  implicit val actorMaterializer = ActorMaterializer()

  val bootstrapServers = "localhost:9091"
  val kafkaTopic = "akka_topic"
  val partition = 0
  val subscription = Subscriptions.assignment(new TopicPartition(kafkaTopic, partition))

  val consumerSettings = ConsumerSettings(actorSystem, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServers)
    .withGroupId("akka_streams_group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val producerSettings = ProducerSettings(actorSystem, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)

  val runnableGraph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val tickSource = Source(Seq("1","2","3","4","5"))
    val kafkaSource = Consumer.plainSource(consumerSettings, subscription)
    val kafkaSink = Producer.plainSink(producerSettings)

    val mapToProducerRecord = Flow[String].map(elem => new ProducerRecord[Array[Byte], String](kafkaTopic, elem))
    val mapFromConsumerRecord = Flow[ConsumerRecord[Array[Byte], String]].map(record => record.value().toInt)

    val f1 = builder.add(Flow[Int].map(x=>x*10))
    val f2 = builder.add(Flow[Int].map(x=>x*2))
    val f3 = builder.add(Flow[Int].map(x=>x*3))
    val f4 = builder.add(Flow[(Int,Int,Int)].map(r => r._1 + r._2 + r._3))

    val broadcast = builder.add(Broadcast[Int](3))

    def concatFunc(s1:Int, s2:Int, s3:Int): (Int,Int,Int) ={
      (s1, s2, s3)
    }

    def printss(s: Int): Unit ={
      print(s"$s ")
    }

    val concat = GraphDSL.create() { implicit b ⇒
      val zipFunction = b.add(ZipWith[Int,Int,Int,(Int,Int,Int)](concatFunc _))
      UniformFanInShape(zipFunction.out, zipFunction.in0, zipFunction.in1, zipFunction.in2)
    }

    val sinkFinal = Sink.foreach(printss)

    val zipConcat = builder.add(concat)

    tickSource  ~> mapToProducerRecord   ~> kafkaSink
    kafkaSource ~> mapFromConsumerRecord ~> broadcast

    broadcast.out(0) ~> f1 ~> zipConcat.in(0)
    broadcast.out(1) ~> f2 ~> zipConcat.in(1)
    broadcast.out(2) ~> f3 ~> zipConcat.in(2)

    zipConcat.out ~> f4 ~> sinkFinal

    ClosedShape
  })

  runnableGraph.run()

}