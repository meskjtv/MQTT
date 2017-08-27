/*This code is based on an example found here: https://gist.github.com/m2mIO-gister/5275324. Logic to store values, run simple regression and predict
 * the next value is added by myself.*/

import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.apache.commons.math3.stat.regression.SimpleRegression;


public class SimpleMqttClient implements MqttCallback {

  MqttClient myClient;
	MqttConnectOptions connOpt;

	public Double[] valueColl = new Double[5];
	public Double[] timeSColl = new Double[5];
	public int n = 0;
	public double predValue;
	static final String BROKER_URL = "tcp://test.mosquitto.org:1883";
	static final String M2MIO_THING = "<Unique device ID>";

	// the following two flags control whether this example is a publisher, a subscriber or both
	static final Boolean subscriber = true;
	static final Boolean publisher = true;

	/**
	 * 
	 * connectionLost
	 * This callback is invoked upon losing the MQTT connection.
	 * 
	 */
	@Override
	public void connectionLost(Throwable t) {
		System.out.println("Connection lost!");
		// code to reconnect to the broker would go here if desired
	}

	/**
	 * 
	 * deliveryComplete
	 * This callback is invoked when a message published by this client
	 * is successfully received by the broker.
	 * 
	 */
	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		try {
			System.out.println("Pub complete" + new String(token.getMessage().getPayload()));
		} catch (MqttException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * messageArrived
	 * This callback is invoked when a message is received on a subscribed topic.
	 * 
	 */
	@Override	
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		
		String payload = new String(message.getPayload());
		payload.getBytes();
		
		//Logic to get the value and timestamp out from the payload. 
		Double value = Double.parseDouble(payload.substring(53, 70));
		String timeStamp = payload.substring(15, 41);
		
		//add 5 values and timeStamp (1-5) into arrays 
		if (n<5) {
			valueColl[n] = value;
			timeSColl[n] = (double)n+1;
			
			System.out.println("-------------------------------------------------");
			System.out.println("| Topic:" + topic);
			System.out.println("| Message: " + new String(message.getPayload()));
			System.out.println(value);
			System.out.println(timeStamp);
			
			for (Double s:valueColl) {
			System.out.println("values in array" + s + " "+ n);
			}
			
			System.out.println("-------------------------------------------------");
		}
			
		//Logic to run simple linear regression when there are 5 values in the array. Uses 1-5 as timestamp instead of timestamp from payload.
		//Predicts the value for the 6th timestamp
		SimpleRegression sReg = new SimpleRegression();
		if(n==4) {
			for(int i = 0; i<valueColl.length; i++) {
				sReg.addData(timeSColl[i],valueColl[i]);
			}
			System.out.println("slope = " + sReg.getSlope());
	        System.out.println("intercept = " + sReg.getIntercept());
	        predValue = sReg.predict(5.0);
	        System.out.println("predict = " + predValue);
	        //publisher = true;
	        
		}	
		
		
		n++;
	}

	/**
	 * 
	 * MAIN
	 * 
	 */
	public static void main(String[] args) {
		SimpleMqttClient smc = new SimpleMqttClient();
		smc.runClient();
	}
	
	/**
	 * 
	 * runClient
	 * Create a MQTT client, connect to broker, pub/sub, disconnect.
	 * 
	 */
	public void runClient() {
		// setup MQTT Client
		String clientID = M2MIO_THING;
		connOpt = new MqttConnectOptions();
		
		connOpt.setCleanSession(true);
		connOpt.setKeepAliveInterval(30);
		
		
		// Connect to Broker
		try {
			myClient = new MqttClient(BROKER_URL, clientID);
			myClient.setCallback(this);
			myClient.connect(connOpt);
		} catch (MqttException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		System.out.println("Connected to " + BROKER_URL);

		// setup topic
		String myTopic = "meters/lyse-test-01";
		MqttTopic topic = myClient.getTopic(myTopic);
		String pubTopic ="predictions/lyse-test-01";
		MqttTopic pTopic = myClient.getTopic(pubTopic);
		

		// subscribe to topic if subscriber
		if (subscriber) {
			try {
				int subQoS = 0;
				myClient.subscribe(myTopic, subQoS);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		
		
			// publish messages if publisher
			if (publisher) {
				//for (int i=1; i<=10; i++) {
			   		//String pubMsg = "{\"timestamp\":" + i + " }";
				//Hard coded timestamp and value for publishing
			   		String pubMsg = "{\"timestamp\":" + "\"2017-08-27T10:12:32.653123\"" + "," + "\"value\": " + "1.979158050576571 }";
			   		int pubQoS = 0;
					MqttMessage message = new MqttMessage(pubMsg.getBytes());
			    	message.setQos(pubQoS);
			    	message.setRetained(false);

			    	// Publish the message
			    	System.out.println("Publishing to topic \"" + pTopic + "\" qos " + pubQoS);
			    	MqttDeliveryToken token = null;
			    	try {
			    		// publish message to broker
						token = pTopic.publish(message);
				    	// Wait until the message has been delivered to the broker
						token.waitForCompletion();
						Thread.sleep(100);
					} catch (Exception e) {
						e.printStackTrace();
					}
				//}	
				
			}
		
		
		
		// disconnect
/*		try {
			// wait to ensure subscribed messages are delivered
			if (subscriber) {
				Thread.sleep(5000);
			}
			myClient.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}*/
	}
}
