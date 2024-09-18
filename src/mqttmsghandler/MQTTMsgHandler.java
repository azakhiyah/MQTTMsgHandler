
package mqttmsghandler;


import java.io.IOException;
import org.eclipse.paho.client.mqttv3.MqttException;
import mqttmsghandler.controller.*;

/**
 *
 * @author zakhiyah arsal
 */
public class MQTTMsgHandler {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, MqttException, InterruptedException{
        while (true) {
            ClientSubscriber.processData();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
    
}
