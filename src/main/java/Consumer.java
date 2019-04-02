import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/***
 * consumer class , it consumes messages produced by the producer
 */
public class Consumer {
    public static void main(String[] args) {
        int numConsumer = 1;
        String groupId = "group-test";
        final ExecutorService executorService = Executors.newFixedThreadPool(numConsumer);
        //reading the topic from the console
        if (args.length != 1) {
            System.err.println("Please specify 1 parameters ");
            System.exit(-1);
        }
        //topic name
        String topicName = args[0];
        //list of topics that the consumer is subscribed to
        List topics = new ArrayList();
        topics.add(topicName);
        //list of threads
        final List<ConsumerLoop> consumers = new ArrayList<ConsumerLoop>();

        for(int i=0;i<numConsumer;i++){
            //new thread , it takes an id , a groupid and a topic to consume messages
            ConsumerLoop consumer = new ConsumerLoop(i,groupId,topics);
            //adding a consumer to the thread pool
            consumers.add(consumer);
            //execute the thread
            executorService.submit(consumer);
        }
        //one the process is terminated, we shutdown the threads as well as the executorService.
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
                for(ConsumerLoop consumer:consumers){
                    consumer.shutdown();
                }
                executorService.shutdown();
                try{
                    executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });

    }
}
