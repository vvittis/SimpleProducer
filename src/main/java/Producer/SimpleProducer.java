package Producer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.swing.*;


//Create java class named “SimpleProducer”
public class SimpleProducer {

    public static void main(String[] args) throws Exception {


        //Assign topicName to string variable
        String topicName = "SineTopic";
        /* SeaDriftTopic ElectricityTopic SineTopic */
        // create instance for properties to access producer configs
        Properties props = new Properties();


        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");


        //Set acknowledgements for producer requests.
        //The acks config controls the criteria under which requests are considered complete.
        //The "all" setting we have specified will result in blocking on the full commit of the record, the slowest but most durable setting.
        props.put("acks", "all");


        //If the request fails, the producer can automatically retry
        props.put("retries", 0);


        //Kafka uses an asynchronous publish/subscribe model.
        //The producer consists of a pool of buffer space that holds records that haven't yet been transmitted to the server.Turning these records into requests and transmitting them to the cluster.
        //The send() method is asynchronous.
        //When called it adds the record to a buffer of pending record sends and immediately returns.
        //This allows the producer to batch together individual records for efficiency.


        //Specify buffer size in config
        //The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by the batch.size config
        //Controls how many bytes of data to collect before sending messages to the Kafka broker.
        //Set this as high as possible, without exceeding available memory.
        //The default value is 16384.
        props.put("batch.size", 16384);


        //Reduce the number of requests less than 0
        //linger.ms sets the maximum time to buffer data in asynchronous mode
        //By default, the producer does not wait. It sends the buffer any time data is available.
        //E.g: Instead of sending immediately, you can set linger.ms to 5 and send more messages in one batch.
        //This would reduce the number of requests sent, but would add up to 5 milliseconds of latency to records sent, even if the load on the system does not warrant the delay.
        props.put("linger.ms", 0);


        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);


        //key-serializer -> string
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //value-serializer -> string
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //Producer
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        //Read the file line by line and send to topic topicName
        BufferedReader br = new BufferedReader(new FileReader("data_source/Concept_Drift_Datasets/sine1_w_50_n_01/sine1_w_50_n_0.1_101.csv")); // input file
        String line = br.readLine();
        int count = 0;
        int counter_class1 = 0;
        int counter_class2 = 0;
        int temp_count = 0;
        int temp_count1 = 0;
        //System.out.println(line);
        while (line != null) {
            //System.out.println(line);
            /*BANK NOTES*/
            count++;
            String keyLine = line.trim();
            String[] instance_class = line.split(",");
            // Testing tuples
            if (count > 0  && instance_class[instance_class.length-1].equals("1") && Math.random() > 0.999 && counter_class1 < 100) {
                counter_class1++;
                keyLine = keyLine.concat(",-5").concat(",").concat(Integer.toString(count));
            } else if (count > 0 && instance_class[instance_class.length-1].equals("0") && Math.random() > 0.999 && counter_class2 < 100) {
                counter_class2++;
                keyLine = keyLine.concat(",-5").concat(",").concat(Integer.toString(count));
            }
            // Training tuples
            else {
                keyLine = keyLine.concat(",5").concat(",").concat(Integer.toString(count));
            }
            System.out.println(keyLine);
            producer.send(new ProducerRecord<String, String>(topicName, String.valueOf(count), keyLine));
            line = br.readLine();
        }
        System.out.println("Counter1: " + counter_class1);
        System.out.println("Counter2: " + counter_class2);
        System.out.println("Real Counter 1: " + temp_count1);
        System.out.println("Real Counter 2: " + temp_count);
        br.close();
        producer.close();

    }

}
