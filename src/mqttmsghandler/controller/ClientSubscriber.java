
package mqttmsghandler.controller;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONObject;

import mqttmsghandler.utilities.Config;
import mqttmsghandler.utilities.MariaDBSQLConnection;

/**
 *
 * @author zakhiyah arsal
 */

public class ClientSubscriber {
     static Logger mLog = Logger.getLogger(ClientSubscriber.class.getName());
     public static void processData() throws IOException, InterruptedException {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Config properties = new Config();
        String broker = new String(properties.getProperty("mqqt.broker"));
        String topic = new String(properties.getProperty("mqqt.topic"));
        String user = new String(properties.getProperty("mqqt.user"));
        String passwrd = new String(properties.getProperty("mqtt.passwrd"));
        String clientId = "subscriber";
        MemoryPersistence persistence = new MemoryPersistence();
        
        try {
            MqttClient mqttClient = new MqttClient(broker, clientId, persistence);
            
             // Create connection option with authentication information
            MqttConnectOptions connectOptions = new MqttConnectOptions();
            connectOptions.setCleanSession(true);
            connectOptions.setUserName(user);
            connectOptions.setPassword(passwrd.toCharArray());

            // Connected to MQTT Broker with authentication information
            mqttClient.connect(connectOptions);
            // Create callback for receive message sent to topic
            mqttClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    mLog.info("Connection lost!");
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    Date date = new Date();
                    String ArrivedTime = sf.format(date);

                    mLog.info(ArrivedTime +" Received message: " + new String(message.getPayload()));
                    String dataReceive = new String (message.getPayload(),"UTF-8");
                    JSONObject jo;
                    jo = new JSONObject(dataReceive);
                    String SiteId = jo.getString("SiteId");
                    int ConsoleId = jo.getInt("ConsoleId");
                    String readTimeSite = jo.getString("ReadTime");
                    //mLog.info(" ReadTimeJSON: " + readTimeSite);
                    String ProductID = jo.getString("ProductID");
                    int TankNo = jo.getInt("TankNo");
                    JSONObject data = (JSONObject) jo.get("Data");
                    float Mass = data.getFloat("Mass");
                    float Temperature = data.getFloat("Temperature");
                    float Volume = data.getFloat("Volume");
                    float VolumeFlowrate = data.getFloat("VolumeFlowrate");
                    float Level = data.getFloat("Level");
                    float GSV = data.getFloat("GSV");
                    float Density = data.getFloat("Density");
                    int status = jo.getInt("Status");
                    if (status == 20) {
                    MariaDBSQLConnection.processedTankData(TankNo, ConsoleId, SiteId,readTimeSite,ProductID ,Level, Volume, Density, Temperature,Mass,VolumeFlowrate,Mass,GSV,status);
                    MariaDBSQLConnection.insertTankDataHistory(TankNo, ConsoleId, SiteId,readTimeSite,ProductID,Level, Volume, Density, Temperature,Mass,VolumeFlowrate,Mass,GSV,status);
                    } else {
                    mLog.info("Save Data Pending from Site");    
                    MariaDBSQLConnection.insertTankDataHistory(TankNo, ConsoleId, SiteId,readTimeSite,ProductID,Level, Volume, Density, Temperature,Mass,VolumeFlowrate,Mass,GSV,20);
                    }
                 }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });
             // Subscribe to MQTT Topic
            mqttClient.subscribe(topic);

            // Tunggu hingga mendapatkan pesan dari topik
            Thread.sleep(100000);
            mqttClient.unsubscribe(topic);
            mqttClient.disconnect();
            mqttClient.close();

            
            
        } catch(MqttException  e) {
            e.printStackTrace();
        }
         
     }
    
}
