package spark;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import java.util.Collections;
import java.util.Set;
import org.apache.spark.SparkException;
import org.codehaus.jettison.json.JSONException;

public class Main {

    public static void main(String[] args) {
        final String appName = "kafka-consumer";
        final String master = "local[*]";
        final String zookeper = "localhost:9092";
        Set<String> topics = Collections.singleton("sspark");

        final String cassandraTable = "testSpark";
        final String cassandraKeySpace = "demoCassandra";

        /*configure cassandra's table and keyspace which we had to create before*/
        SparkCassandraWriter.configure(cassandraTable, cassandraKeySpace);
        /* create new spark application and start it*/
        try {
            SparkKafkaReader cw = new SparkKafkaReader(appName, master);
            cw.start(zookeper, topics);
        } catch (JSONException ex) {
            System.out.println("-------Error in json parse: " + ex);
        } catch (SparkException ex) {
            System.out.println("-------error in spark : " + ex);
        } catch (QueryValidationException ex) {
            System.out.println("-------wrong keyspace name. error: " + ex);
        } catch (NoHostAvailableException ex) {
            System.out.println("-------connection error: " + ex);
        }
    }

}
