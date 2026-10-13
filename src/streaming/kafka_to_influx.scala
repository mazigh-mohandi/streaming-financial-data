import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

// ---- Kafka
val brokers = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
val topic   = "crypto_trades"

// ---- Influx v2
val influxUrl    = sys.env.getOrElse("INFLUXDB_URL", "http://localhost:8086")
val influxToken  = sys.env.getOrElse("INFLUXDB_TOKEN", sys.error("INFLUXDB_TOKEN is required"))
val influxOrg    = sys.env.getOrElse("INFLUXDB_ORG", "esilv")
val influxBucket = sys.env.getOrElse("INFLUXDB_BUCKET", "trading")
val writeEndpoint = s"$influxUrl/api/v2/write?org=$influxOrg&bucket=$influxBucket&precision=ms"

// ---- Schema (messages finnhub)
val valueSchema = StructType(Seq(
  StructField("source", StringType),
  StructField("asset_class", StringType),
  StructField("event_type", StringType),
  StructField("symbol", StringType),
  StructField("exchange", StringType),
  StructField("pair", StringType),
  StructField("price", DoubleType),
  StructField("volume", DoubleType),
  StructField("notional", DoubleType),
  StructField("event_ts_ms", LongType),
  StructField("ingest_ts_ms", LongType),
  StructField("open", DoubleType),
  StructField("high", DoubleType),
  StructField("low", DoubleType),
  StructField("close", DoubleType),
  StructField("prev_close", DoubleType),
  StructField("quote_event_ts_ms", LongType),
  StructField("quote_ingest_ts_ms", LongType)
))

// ---- Read Kafka
val rawDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", brokers)
  .option("subscribe", topic)
  .option("startingOffsets", "latest")
  .load()

// ---- Parse JSON
val parsed = rawDF
  .selectExpr("CAST(value AS STRING) AS value_str")
  .withColumn("json", from_json(col("value_str"), valueSchema))
  .select("json.*")
  .filter(col("event_ts_ms").isNotNull)
  .filter(col("symbol").isNotNull)

// ---- Escape tag values
def escTag(s: String): String =
  s.replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")

// ---- Influx line protocol
def toLine(row: org.apache.spark.sql.Row): String = {
  val symbol   = escTag(row.getAs[String]("symbol"))
  val exchange = escTag(row.getAs[String]("exchange"))
  val pair     = escTag(row.getAs[String]("pair"))

  val price    = row.getAs[Double]("price")
  val volume   = row.getAs[Double]("volume")
  val notional = row.getAs[Double]("notional")
  val open     = row.getAs[Double]("open")
  val high     = row.getAs[Double]("high")
  val low      = row.getAs[Double]("low")
  val close    = row.getAs[Double]("close")
  val prev     = row.getAs[Double]("prev_close")
  val ts       = row.getAs[Long]("event_ts_ms") // ms

  s"crypto_trades,symbol=$symbol,exchange=$exchange,pair=$pair price=$price,volume=$volume,notional=$notional,open=$open,high=$high,low=$low,close=$close,prev_close=$prev $ts"
}

val http = HttpClient.newHttpClient()

def writeToInflux(batchDF: DataFrame, batchId: Long): Unit = {
  val rows = batchDF.limit(2000).collect()
  if (rows.nonEmpty) {
    val body = rows.map(toLine).mkString("\n")

    val req = HttpRequest.newBuilder()
      .uri(URI.create(writeEndpoint))
      .header("Authorization", s"Token $influxToken")
      .header("Content-Type", "text/plain; charset=utf-8")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()

    val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
    if (resp.statusCode() != 204) {
      System.err.println(s"[Influx WRITE ERROR] status=${resp.statusCode()} body=${resp.body()}")
    } else {
      println(s"[Influx OK] batchId=$batchId points=${rows.length}")
    }
  }
}

// ---- Checkpoint (HDFS conseillé)
val checkpoint = "hdfs://master:9000/tmp/chk_kafka_to_influx_crypto"

val q = parsed.writeStream
  .foreachBatch(writeToInflux _)
  .outputMode("append")
  .option("checkpointLocation", checkpoint)
  .start()

q.awaitTermination()