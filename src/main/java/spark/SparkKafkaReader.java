package spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

public class SparkKafkaReader {

    private String appName;
    private String master;
    private JavaSparkContext sparkContext;
    private SparkConf configuration;
    private JavaStreamingContext javaStreamContext;

    public SparkKafkaReader(String appName, String master) {
        this.appName = appName;
        this.master = master;
        configuration = new SparkConf()
                .setAppName(appName)
                .setMaster(master);
        sparkContext = new JavaSparkContext(configuration);

    }

    /*to start consuming from kafka*/
    public void start(String zookeeper, Set<String> topics) throws SparkException {

        /*congigure new spark context*/
        javaStreamContext = new JavaStreamingContext(sparkContext, new Duration(10000));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", zookeeper);

        /*create new direct stream*/
        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(javaStreamContext,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
            System.out.println("--- New RDD with " + rdd.partitions().size()
                    + " partitions and " + rdd.count() + " records");
            if (rdd.count() > 0) { //if our rdd consumed new records, we send them to cassandra
                rdd.foreach(record -> System.out.println(record._2)); //if records exist, show them to user                
                List<String> dataList = new ArrayList<String>();
                for (Tuple2<String, String> entry : rdd.collect()) {
                    dataList.add(entry._2());
                }
                System.out.println("====10 lines will be sent to cassandra = " + dataList.size());

                try {
                    int number = SparkCassandraWriter.sendFromJson(dataList);
                    System.out.println(number + "lines was inserted");
                } catch (Exception ex) {
                    System.err.println("-------error happened while sending data to cassandra: " + ex);
                }

            } else {
                System.out.println("-----------------------");
            }

        });

        javaStreamContext.start();
        javaStreamContext.awaitTermination();

    }

    /*to stop spark context before we can create a new one*/
    public void stopSparkContext() {
        if (sparkContext != null) {
            sparkContext.stop();
        }
    }

    /*to stop stream context. */
    public void stopConsuming() {
        if (javaStreamContext != null) {
            javaStreamContext.stop();
        }
    }

    /*to add a new kafka topic. just in case :)  */
    public void addKafkaTopic() {
        //not realized yet
    }

}
