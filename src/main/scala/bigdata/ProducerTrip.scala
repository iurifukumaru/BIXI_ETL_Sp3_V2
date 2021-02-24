package bigdata

import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

import java.util.Properties
import scala.io.Source

object ProducerTrip extends App with HDFS  {

    if(fs.delete(new Path(s"$uri/user/winter2020/iuri/sprint3"),true))
        println("Folder Sprint3 deleted before Instantiate!")

    fs.copyFromLocalFile(new Path(
        "file:///home/iuri/Documents/FinalProject/sprint3v2/src/main/resources/100_trips.csv"),
        new Path(s"$uri/user/winter2020/iuri/sprint3/100_trips.csv"))

    val topicName = "winter2020_iuri_trip"
    val producerProperties = new Properties()
    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer = new KafkaProducer[Int, String](producerProperties)

    val inputStream: FSDataInputStream = fs.open(new Path("/user/winter2020/iuri/sprint3/100_trips.csv"))

    var tripKey = 1
    val tripMessage = Source.fromInputStream(inputStream).getLines().toList
      .foreach(line => {
        println(line)
        producer.send(new ProducerRecord[Int, String](topicName, tripKey, line))
          tripKey = tripKey + 1
      })
    producer.flush()

}

