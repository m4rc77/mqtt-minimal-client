package ch.m4rc.mqtt;

import java.util.UUID;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;

/**
 * https://github.com/hivemq/hivemq-mqtt-client
 * https://www.hivemq.com/blog/mqtt-client-library-enyclopedia-hivemq-mqtt-client/
 */
public class MqttMinimalClient {

    private static final Logger LOG = LoggerFactory.getLogger(MqttMinimalClient.class);

    private static final long CONNECT_SLEEP = 1000;

    private static final String DEFAULT_SERVER = "broker.hivemq.com";
    private static final String DEFAULT_TOPIC = "test/#";
    private static final int PORT = 1883;

    private static Mqtt3AsyncClient client;

    /**
     * Runs the mqtt minimal mqtt client ...
     *
     * @param args pass as first argument the topic and as second the server to subscribe.
     */
    public static void main(String[] args) {
        try {
            String topic = args.length > 0 && !args[0].isEmpty() ? args[0] : DEFAULT_TOPIC;
            String server = args.length > 1 && !args[1].isEmpty() ? args[1] : DEFAULT_SERVER;

            LOG.info("Start MQTT client with ...");
            LOG.info("   - topic: " + topic);
            LOG.info("   - server: " + server + ":" + PORT);

            client = MqttClient.builder()
                    .useMqttVersion3()
                    .identifier("MMC_" + UUID.randomUUID().toString())
                    .serverHost(server)
                    .serverPort(PORT)
                    .buildAsync();

            connect();
            subscribe(topic);

            //noinspection InfiniteLoopStatement
            while (true) {
                System.out.println(".");
                sleep();
            }

        } catch (Exception e) {
            LOG.error("MQTT client failed", e);
            System.exit(99);
        }
    }

    public static void connect() {
        Future<Mqtt3ConnAck> f = client.connect()
//        Future<Mqtt3ConnAck> f = client.connectWith()
//                .willPublish()
//                    .topic(willTopic)
//                    .payload(willMsg.getBytes())
//                    .qos(MqttQos.AT_LEAST_ONCE)
//                    .retain(false)
//                    .applyWillPublish()
//                .send()
                .whenComplete((connAck, throwable) -> {
                    if (throwable != null) {
                        LOG.error("MQTT connect failed", throwable);
                        System.exit(98);
                    } else {
                        LOG.info("Connected to " + DEFAULT_SERVER + ":" + PORT);
                    }
                });

        while (!f.isDone()) {
            LOG.info("Wait for MQTT connect ...");
            sleep();
        }
    }

    public static void subscribe(String topic) {
        client.subscribeWith()
                .topicFilter(topic)
                .callback(publish -> {
                    LOG.info("mqtt: " + new String(publish.getPayloadAsBytes()));
                })
                .send()
                .whenComplete((subAck, throwable) -> {
                    if (throwable != null) {
                        LOG.error("MQTT subscribe failed", throwable);
                        System.exit(97);
                    } else {
                        LOG.info("Subscribed to topic " + topic);
                    }
                });

    }

    private static void sleep() {
        try {
            Thread.sleep(CONNECT_SLEEP);
        } catch (InterruptedException e) {
            // ignore
        }
    }

}
