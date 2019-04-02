import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.List;
import java.util.Properties;

/***
 * The consumer Thread
 */
public class ConsumerLoop implements Runnable {
    private KafkaConsumer<String ,String > consumer;
    private List<String> topics;
    private int id;

    public ConsumerLoop(int id,String groupId , List<String> topics){
        this.id = id;
        this.topics = topics;
        Properties props = new Properties();
        // Set the brokers (bootstrap servers)
        props.setProperty("bootstrap.servers", "10.75.17.84:9092");
        // Set how to deserialize key/value pairs
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //the group id
        props.put("group.id",groupId);
        //the kafka consumer
        this.consumer = new KafkaConsumer<String, String>(props);
    }

    /***
     * This method consume messages from the broker (messages produced by the kafka producer) and print them.
     */
    public void run() {
        try{
            consumer.subscribe(topics);
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for(ConsumerRecord<String, String> record: records){
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s  %s", record.topic(), record.partition(),record.key() ,  record.value()));
                }

            }
        }catch (WakeupException e){

        }finally {
            consumer.close();
        }

    }
    // free resources
    public void shutdown(){
        consumer.wakeup();
    }
}
