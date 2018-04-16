package co.edu.uniandes.mati.nuevageneracion.taller3.simulador;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * Producer Example in Apache Kafka
 * @author www.tutorialkart.com
 * @author Alex Vicente Chacon Jimenez (alex.chacon@gmail.com)
 * @author Gabriel Zapata (gab.j.zapata@gmail.com)
 * @author Andres Sarmiento (ansarmientoto@gmail.com)
 */
public class SampleProducer extends Thread
{
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public SampleProducer(String topic, Boolean isAsync, String kafkaServer, String clientId)
    {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaServer);
        properties.put("client.id", clientId);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    public void run()
    {
        int messageNo = 1;
        Random random = new Random();

        while (true)
        {
            int max = 30;
            int min = 12;

            String messageStr = Integer.toString(random.nextInt(max + 1 - min) + min);

            long startTime = System.currentTimeMillis();
            if (isAsync)
            {
                //
                // Send asynchronously
                producer.send(new ProducerRecord<>(topic, messageNo,  messageStr), new DemoCallBack(startTime, messageNo, messageStr));
            }
            else
            {
                //
                // Send synchronously
                try
                {
                    producer.send(new ProducerRecord<>(topic, messageNo, messageStr)).get();
                    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
                }
                catch (InterruptedException | ExecutionException e)
                {
                    e.printStackTrace();
                }
            }
            ++messageNo;

            try
            {
                sleep(4000);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }
}

class DemoCallBack implements Callback
{
    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message)
    {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * onCompletion method will be called when the record sent to the Kafka Server has been acknowledged.
     *
     * @param metadata  The metadata contains the partition and offset of the record. Null if an error occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception)
    {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null)
        {
            System.out.println( "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() + "), " + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        }
        else
        {
            exception.printStackTrace();
        }
    }
}