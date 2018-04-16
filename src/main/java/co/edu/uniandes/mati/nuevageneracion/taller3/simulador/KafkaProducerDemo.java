package co.edu.uniandes.mati.nuevageneracion.taller3.simulador;

/**
 * Stream producer demo.
 *
 * @author https://www.tutorialkart.com/apache-kafka/producer-example-in-apache-kafka/
 * @author Alex Vicente Chacon Jimenez (alex.chacon@gmail.com)
 * @author Gabriel Zapata (gab.j.zapata@gmail.com)
 * @author Andres Sarmiento (ansarmientoto@gmail.com)
 */
public class KafkaProducerDemo
{

    public static void main(String[] args)
    {
        String topic = args[0];
        String isAsync = args[1];
        String kafkaServer = args[2];
        String clientId = args[3];

        SampleProducer producerThread = new SampleProducer(topic, Boolean.valueOf(isAsync), kafkaServer, clientId);

        //
        // Start the producer
        producerThread.start();
    }
}
