/*
  Copyright 1995-2016 Esri

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

  For additional information, contact:
  Environmental Systems Research Institute, Inc.
  Attn: Contracts Dept
  380 New York Street
  Redlands, California, USA 92373

  email: contracts@esri.com
 */
package com.esri.geoevent.transport.aws;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import com.amazonaws.services.iot.client.AWSIotException;
import com.amazonaws.services.iot.client.AWSIotMessage;
import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.AWSIotQos;
import com.amazonaws.services.iot.client.AWSIotTopic;
import com.esri.geoevent.transport.aws.AwsIoTHubUtil.KeyStorePasswordPair;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.InboundTransportBase;
import com.esri.ges.transport.TransportDefinition;

public class AwsIoTHubInboundTransport extends InboundTransportBase implements Runnable
{	
  //based on the aws-iot-device-sdk-java	
  //https://github.com/aws/aws-iot-device-sdk-java

  // logger
  private static final BundleLogger LOGGER                 = BundleLoggerFactory.getLogger(AwsIoTHubInboundTransport.class);
  
  // transport properties 
  private boolean                   isEventHubType         = true;
  private String                    iotServiceType         = "";
    
  private String                    deviceIdFieldName      = "";
  
  private String 					clientEndpoint		   = "";
  private String					x509Certificate		   = "";
  private String 					privateKey			   = "";
  private String 					topicName			   = "";
  
  // data members
  private AWSIotMqttClient 			awsClient			   = null;
  private AwsIoTHubDevice 			geIoTDevice			   = null;
  
  private String                    errorMessage;
  private Thread					thread				   = null;
  private volatile boolean          propertiesNeedUpdating = false;
  
  public enum AwsIoTServiceType {
	  IOT_TOPIC,
	  IOT_DEVICE
   };

  public AwsIoTHubInboundTransport(TransportDefinition definition) throws ComponentException
  {
    super(definition);
  }

  @SuppressWarnings("incomplete-switch")
  public void start() throws RunningException
	{
		try
		{
			switch (getRunningState())
			{
				case STARTING:
				case STARTED:
				case STOPPING:
					return;
			}
			setRunningState(RunningState.STARTING);
			thread = new Thread(this);
			thread.start();
		}
		catch (Exception e)
		{
			LOGGER.error("UNEXPECTED_ERROR_STARTING", e);
			stop();
		}
	}
  
  
  @Override
  public void run() {
	  connectToAwsEventHub();
  }
  
  private void connectToAwsEventHub()
  {
    String errorMessage = null;
    RunningState runningState = RunningState.STARTED;

    try
    {
      applyProperties();
      if (propertiesNeedUpdating)
      {
        cleanup();
        propertiesNeedUpdating = false;
      }

      // Setup
      isEventHubType = AwsIoTServiceType.IOT_TOPIC.toString().equals(iotServiceType);
      
      //AWS Event Hub       
      KeyStorePasswordPair pair = AwsIoTHubUtil.getKeyStorePasswordPair(x509Certificate, privateKey, null);
      awsClient = new AWSIotMqttClient(clientEndpoint, deviceIdFieldName , pair.keyStore, pair.keyPassword);
      
      if (awsClient == null)
      {
        runningState = RunningState.ERROR;
        errorMessage = LOGGER.translate("FAILED_TO_CREATE_EH_CLIENT", clientEndpoint);
        LOGGER.error(errorMessage);
      }        
      
      if (!isEventHubType)
      {         	
        // IoT Device
    	geIoTDevice = new AwsIoTHubDevice(deviceIdFieldName);
    	awsClient.attach(geIoTDevice);    	      
      }      
      awsClient.connect();
      
      //delete existing shawdow if any
      if(geIoTDevice != null){
    	  geIoTDevice.delete();
      }
      
      AWSIotTopic topic = new AwsIoTTopicListener(topicName, AWSIotQos.QOS0);
      awsClient.subscribe(topic, true);

      setErrorMessage(errorMessage);
      setRunningState(runningState);
    }
    catch (AWSIotException iote)
    {
      LOGGER.error("AWSIOT_INIT_ERROR", iote);      
      setErrorMessage(iote.getMessage());
      setRunningState(RunningState.ERROR);
    }
    catch (Exception ex)
    {
      LOGGER.error("INIT_ERROR", ex);      
      setErrorMessage(ex.getMessage());
      setRunningState(RunningState.ERROR);
    }
  }

  @Override
  public synchronized void stop()
  {   
    errorMessage = null;   
    cleanup();
    setErrorMessage(null);
    setRunningState(RunningState.STOPPED);
  }

  private void cleanup()
  {     
      if (awsClient != null)
      {
        try
        {
        	if (geIoTDevice != null) {       
        		awsClient.detach(geIoTDevice);
        		geIoTDevice.delete(5000);
        	}
        	awsClient.disconnect(5000);        
        }
        catch (Exception e)
        {
          LOGGER.error("CLEANUP_ERROR",e);                       
  		  setErrorMessage(e.getMessage());
        }finally{
	        geIoTDevice = null;
	        awsClient = null;        
        } 
      }    
  }

  private void applyProperties() throws Exception
  {    
      boolean somethingChanged = false;
            
      //iot service type; Event Hub or Device
      if (hasProperty("iotservicetype"))
      {
        // IoT Service Type
        String newIotServiceType = getProperty("iotservicetype").getValueAsString();
        if (!iotServiceType.equals(newIotServiceType))
        {
          iotServiceType = newIotServiceType;
          somethingChanged = true;
        }
      }  
      // Device Id Field Name
      if (hasProperty("deviceid"))
      {
        String newDeviceIdFieldName = getProperty("deviceid").getValueAsString();
        if (!deviceIdFieldName.equals(newDeviceIdFieldName))
        {
          deviceIdFieldName = newDeviceIdFieldName;
          somethingChanged = true;
        }
      }
      // Client End point
      if (hasProperty("endpoint"))
      {
        String newClientEndpoint  = getProperty("endpoint").getValueAsString();
        if (!clientEndpoint.equals(newClientEndpoint))
        {
          clientEndpoint = newClientEndpoint;
          somethingChanged = true;
        }
      }     
      //X.509 certificate
      if (hasProperty("X509certificate"))
      {
        String newX509Cert  = getProperty("X509certificate").getValueAsString();
        if (!x509Certificate.equals(newX509Cert))
        {
         x509Certificate = newX509Cert;
         somethingChanged = true;
        }
      }
     //private key
      if (hasProperty("privateKey"))
      {
        String newPrivateKey  = getProperty("privateKey").getValueAsString();
        if (!privateKey.equals(newPrivateKey))
        {
         privateKey = newPrivateKey;
         somethingChanged = true;
        }
      }
      //topic name
      if (hasProperty("topic"))
      {
        String newTopicName  = getProperty("topic").getValueAsString();
        if (!topicName.equals(newTopicName))
        {
         topicName = newTopicName;
         somethingChanged = true;
        }
      }            
      
      propertiesNeedUpdating = somethingChanged;   
  }
  
  private void receive(byte[] bytes)
  {
    if (bytes != null && bytes.length > 0)
    {
      String str = new String(bytes);
      str = str + '\n';
      byte[] newBytes = str.getBytes();

      ByteBuffer bb = ByteBuffer.allocate(newBytes.length);
      try
      {
        bb.put(newBytes);
        bb.flip();
        byteListener.receive(bb, "");
        bb.clear();
      }
      catch (BufferOverflowException boe)
      {
        LOGGER.error("BUFFER_OVERFLOW_ERROR", boe);
        bb.clear();
        setRunningState(RunningState.ERROR);
      }
      catch (Exception e)
      {
        LOGGER.error("UNEXPECTED_ERROR", e);
        stop();
        setRunningState(RunningState.ERROR);
      }
    }
  }

 
  /**
   * AwsIoTTopicListener class extends {@link AWSIotTopic} to receive messages from a subscribed
   * topic.
   */
  public final class AwsIoTTopicListener extends AWSIotTopic {

      public AwsIoTTopicListener(String topic, AWSIotQos qos) {
          super(topic, qos);
      }

      @Override
      public void onMessage(AWSIotMessage message) {
          System.out.println(System.currentTimeMillis() + ": <<< " + message.getStringPayload());
          receive(message.getPayload());
      }
  }

  @Override
  public String getStatusDetails()
  {
    return errorMessage;
  }

  @Override
  public boolean isClusterable()
  {
    return false;
  }

}