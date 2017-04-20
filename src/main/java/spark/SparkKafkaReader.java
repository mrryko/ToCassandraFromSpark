package spark;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.codehaus.jettison.json.JSONException;

public class SparkKafkaReader {

    private JavaSparkContext sparkContext;
    private SparkConf configuration;
    private JavaStreamingContext javaStreamContext;

    public SparkKafkaReader(String appName, String master) {
        configuration = new SparkConf()
                .setAppName(appName)
                .setMaster(master);
        sparkContext = new JavaSparkContext(configuration);

    }

    /*starts consuming from kafka*/
    public void start(String zookeeper, Set<String> topics) throws SparkException,
            NoHostAvailableException, QueryValidationException, JSONException {
        /*configures new spark context*/
        javaStreamContext = new JavaStreamingContext(sparkContext, new Duration(10000));
        Map<String, String> kafkaParams = Collections.singletonMap("metadata.broker.list", zookeeper);
        /*create a new direct stream*/
        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(javaStreamContext,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
            System.out.println("--- New RDD with " + rdd.partitions().size()
                    + " partitions and " + rdd.count() + " records");
            if (rdd.count() > 0) { //if rdd consumed new records, sends them to cassandra
                rdd.foreach(record -> System.out.println(record._2)); //if records exist, shows them to user 
                List<String> dataList = rdd.collect().parallelStream()
                        .map(e -> e._2())
                        .collect(Collectors.toList());

                int number = SparkCassandraWriter.sendFromJson(dataList);
                System.out.println(number + " lines were inserted");

            } else {
                System.out.println("-----------------------");
            }

        });
                
        javaStreamContext.start();
        javaStreamContext.awaitTermination();
    }

    /*stops spark context before we can create a new one*/
    public void stopSparkContext() {
        if (sparkContext != null) {
            sparkContext.stop();
        }
    }

    /*stopss stream context. */
    public void stopConsuming() {
        if (javaStreamContext != null) {
            javaStreamContext.stop();
        }
    }

    /*TODO: add a new kafka topic. just in case :)  */
    public void addKafkaTopic() {
        //not realized yet
    }

}
