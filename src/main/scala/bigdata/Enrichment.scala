package bigdata

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties

object Enrichment extends App with HDFS {

  val spark = SparkSession.builder().appName("Sprint 3 Project").master("local[*]").getOrCreate()

  val fileSI = s"$uri/user/hive/warehouse/winter2020_iuri.db/enriched_station_information/000000_0"
  val stationInformationRdd = spark.sparkContext.textFile(fileSI).map(fromCsv => StationsInfo(fromCsv))
  import spark.implicits._
  val stationInformationDf = stationInformationRdd.toDF()
  stationInformationDf.printSchema()
  stationInformationDf.show()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(15))
  val kafkaConfig = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.GROUP_ID_CONFIG -> "group-id-100-trips-records",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")
  val topic = "winter2020_iuri_trip"
  val inStream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](List(topic), kafkaConfig))
  inStream.map(_.value()).foreachRDD(rdd => businessLogic(rdd))

  val srClient = new CachedSchemaRegistryClient("http://localhost:8081", 1)
  val metadata = srClient.getSchemaMetadata("winter2020_iuri_enriched_trip-value", 3)
  val enrichedTripSchema = srClient.getByID(metadata.getId)

  val producerProperties = new Properties()
  producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
  producerProperties.setProperty("schema.registry.url", "http://localhost:8081")

  def businessLogic(rdd: RDD[String]) = {
    val trips: RDD[Trip] = rdd.map(fromCsv => Trip(fromCsv))
    val stopTimeDf = trips.toDF()
    stopTimeDf.createOrReplaceTempView("trip")
    stationInformationDf.createOrReplaceTempView("station")

    val joinResult = spark.sql(
      """
        |SELECT start_date, start_station_code, end_date, end_station_code, duration_sec, is_member,
        |system_id, timezone, station_id, name, short_name, lat, lon, capacity
        |FROM trip LEFT JOIN station ON trip.start_station_code = station.short_name
        |""".stripMargin)
    joinResult.show()
    println(joinResult.collectAsList())

    val enrichedTripIterator = joinResult.collectAsList.listIterator()

    while (enrichedTripIterator.hasNext) {
      val enrichedTripMessage = enrichedTripIterator.next()
        .toString
        .replace("[", "")
        .replace("]", "")
      println(enrichedTripMessage)
      val fields = enrichedTripMessage.split(",")
      val enrichedTripAvro = new GenericRecordBuilder(enrichedTripSchema)
        .set("start_date", fields(0))
        .set("start_station_code", fields(1).toInt)
        .set("end_date", fields(2))
        .set("end_station_code", fields(3).toInt)
        .set("duration_sec", fields(4).toInt)
        .set("is_member", fields(5).toInt)
        .set("system_id", fields(6))
        .set("timezone", fields(7))
        .set("station_id", fields(8).toInt)
        .set("name", fields(9))
        .set("short_name", fields(10))
        .set("lat", fields(11).toDouble)
        .set("lon", fields(12).toDouble)
        .set("capacity", fields(13).toInt)
        .build()
      println(enrichedTripAvro.toString)

      val producer = new KafkaProducer[String, GenericRecord](producerProperties)

      List(enrichedTripAvro)
        .map(avroMessage => new ProducerRecord[String, GenericRecord]
        ("winter2020_iuri_enriched_trip", avroMessage.get("start_station_code").toString, avroMessage))
        .foreach(producer.send)

      producer.flush()
    }
  }

  ssc.start()
  ssc.awaitTermination()
}
