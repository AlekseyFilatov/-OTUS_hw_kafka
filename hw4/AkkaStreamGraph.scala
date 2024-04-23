package hw4.kafka.otus;

 import AkkaStreamGraph.materializer.system
 import akka.{Done, NotUsed}
 import akka.actor.ActorSystem
 import akka.kafka.ProducerSettings

import akka.kafka.scaladsl.Producer
 import akka.remote.serialization.StringSerializer
 import akka.serialization.ByteArraySerializer
 import akka.stream.{ActorMaterializer, ClosedShape, Graph, UniformFanInShape}
 import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source, Zip, ZipWith}
 import org.apache.kafka.clients.producer.ProducerRecord
 import akka.kafka.{ConsumerSettings, Subscriptions}
 import akka.kafka.scaladsl.Consumer
 import akka.util.Helpers.Requiring
 import org.apache.kafka.clients.consumer.ConsumerConfig
 import org.apache.kafka.common.serialization.StringDeserializer
 import org.apache.kafka.common.serialization.IntegerSerializer
 import org.apache.kafka.common.serialization.IntegerDeserializer

 import java.time.Duration

  object AkkaStreamGraph {
    implicit val system: ActorSystem = ActorSystem("system")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val graoh = GraphDSL.create(){ implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val input = builder.add(Source(1 to 5))

      val f1 = builder.add(Flow[Int].map(x=>x*10))
      val f2 = builder.add(Flow[Int].map(x=>x*2))
      val f3 = builder.add(Flow[Int].map(x=>x*3))
      val f4 = builder.add(Flow[(Int,Int,Int)].map(r => r._1 + r._2 + r._3))

      def concatFunc(s1:Int, s2:Int, s3:Int): (Int,Int,Int) ={
        (s1, s2, s3)
      }

      def printss(s1: Int): Unit ={
        print(s"$s1 ")
      }

      val concat = GraphDSL.create() { implicit b ⇒
        val zipFunction = b.add(ZipWith[Int,Int,Int,(Int,Int,Int)](concatFunc _))
        UniformFanInShape(zipFunction.out, zipFunction.in0, zipFunction.in1, zipFunction.in2)
      }

      val sinkFinal = Sink.foreach(printss)

      val broadcast = builder.add(Broadcast[Int](3))

      val zipConcat = builder.add(concat)

      input ~> broadcast

      broadcast.out(0) ~> f1 ~> zipConcat.in(0)
      broadcast.out(1) ~> f2 ~> zipConcat.in(1)
      broadcast.out(2) ~> f3 ~> zipConcat.in(2)

      zipConcat.out ~> f4 ~> sinkFinal

      ClosedShape
    }

    def main(args: Array[String]): Unit = {
      RunnableGraph.fromGraph(graoh).run()
    }

}


