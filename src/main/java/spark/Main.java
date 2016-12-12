package spark;

import java.util.Collections;
import java.util.Set;
import org.apache.spark.SparkException;

public class Main {

    public static void main(String[] args) {
        String appName = "kafka-consumer";
        String master = "local[*]";
        //String master = "local[sdfsdsdf]";
        String zookeper = "localhost:9092";        
        Set<String> topics = Collections.singleton("SparkAggregationTopic");  
        
        String cassandraTable = "testSpark";
        String cassandraKeySpace = "demoCassandra";
        
        /*configure cassandra's table and keyspace which we had to create before*/    
        SparkCassandraWriter.configure(cassandraTable, cassandraKeySpace);  
        /* create new spark application and start it*/
        try{
        SparkKafkaReader cw = new SparkKafkaReader(appName, master);        
        cw.start(zookeper, topics);
        }
        catch(SparkException ex){
            System.out.println("-------error: " + ex);
        }
    }

}
