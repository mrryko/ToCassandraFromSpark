package spark;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import java.util.Iterator;
import java.util.List;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class SparkCassandraWriter {

    private static String table;
    private static String keySpace;

    public static void configure(String table2, String keySpace2) {
        table = table2;
        keySpace = keySpace2;
    }

    public static int sendFromJson(List<String> KafkaStream)
            throws NoHostAvailableException, QueryValidationException, JSONException {
        List<String> directKafkaStream = KafkaStream;
        System.out.println("list created. size: " + directKafkaStream.size());
        int counter = 0; //for number of executed queries

        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

        Session session = cluster.connect();
        try {
            session.execute("USE " + keySpace);
            List<String> list = directKafkaStream;
            Iterator itr = list.iterator();

            while (itr.hasNext()) {
                String message = itr.next().toString();
                JSONObject object = new JSONObject(message);
                String country = object.getString("countryCode");
                int word = object.getInt("numberOfWords");

                PreparedStatement prepared = session.prepare("INSERT INTO " + table
                        + " (countryCode, numberOfWords)"
                        + "VALUES (?,?)");
                BoundStatement bound = prepared.bind(country, word);
                session.execute(bound);
                counter++;
            }
            
            return counter;
            
        } finally {
            session.close();
        }

    }
}
