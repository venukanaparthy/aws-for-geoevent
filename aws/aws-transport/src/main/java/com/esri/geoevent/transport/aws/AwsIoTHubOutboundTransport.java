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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.amazonaws.services.iot.client.AWSIotMessage;
import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.AWSIotQos;
import com.esri.geoevent.transport.aws.AwsIoTHubUtil.KeyStorePasswordPair;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.OutboundTransportBase;
import com.esri.ges.transport.TransportDefinition;
import com.esri.ges.util.Validator;

public class AwsIoTHubOutboundTransport extends OutboundTransportBase
{
  // logger
  private static final BundleLogger LOGGER                 = BundleLoggerFactory.getLogger(AwsIoTHubOutboundTransport.class);

  // transport properties
  private String                    iotServiceType         = "";    
  private String                    deviceIdFieldName      = "";
  
  private String 					clientEndpoint		   = "";  
  private String					x509Certificate		   = "";
  private String 					privateKey			   = "";
  private String 					topicName			   = "";

  private volatile boolean          propertiesNeedUpdating = false;

  private boolean                   isEventHubType         = true;

  // device id client and receiver
  private static GEIoTDevice 		geIoTDevice			   = null;
  // event hub client
  AWSIotMqttClient 					awsClient			   = null;
  AWSIotMessage 					iotMessage			   = null;	
  
  public enum AwsIoTServiceType {
	  IOT_HUB,
	  IOT_DEVICE
   };

  public AwsIoTHubOutboundTransport(TransportDefinition definition) throws ComponentException
  {
    super(definition);
  }

  @Override
  public synchronized void start()
  {
	try{ 	  
    switch (getRunningState())
    {
      case STARTING:
      case STARTED:
        return;
      default:
    }

    setRunningState(RunningState.STARTING);
    setup();
    setRunningState(RunningState.STARTED);
    
	}catch(Exception e){
		LOGGER.error("INIT_ERROR", e);		
		setErrorMessage(e.getMessage());
		setRunningState(RunningState.ERROR);
	}
  }

  private void readProperties()
  {  
      boolean somethingChanged = false;

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

  public synchronized void setup() throws Exception
  {
      readProperties();
      if (propertiesNeedUpdating)
      {
        cleanup();
        propertiesNeedUpdating = false;
      }

      // iot service type - Event Hub or Device
      isEventHubType = AwsIoTServiceType.IOT_HUB.equals(iotServiceType);
      
      //AWS Event Hub       
      KeyStorePasswordPair pair = AwsIoTHubUtil.getKeyStorePasswordPair(x509Certificate, privateKey, null);
      awsClient = new AWSIotMqttClient(clientEndpoint, deviceIdFieldName , pair.keyStore, pair.keyPassword);           
      
      if (!isEventHubType)
      {         	
        // IoT Device
    	geIoTDevice = new GEIoTDevice(deviceIdFieldName);
    	awsClient.attach(geIoTDevice);    	        
      }      
      awsClient.connect();    
  }

  @Override
  public synchronized void stop()
	{
		setRunningState(RunningState.STOPPING);
		cleanup();		
		setRunningState(RunningState.STOPPED);
	}
  
  private void cleanup()
  {
    // clean up the aws hub client       	  
	  try
      {
	         if (awsClient != null)
	         {	        	
	        	if (geIoTDevice !=null) {       
	        		awsClient.detach(geIoTDevice);        		
	        		geIoTDevice.delete();
	        	}
	        	awsClient.disconnect();
	         }
      }
      catch (Exception e)
      {
        LOGGER.error("CLEANUP_ERROR", e);            		  
		setErrorMessage(e.getMessage());  		  
      }finally {
	    geIoTDevice = null;
	    awsClient = null;
      }                   
  }

  @Override
  public void receive(ByteBuffer buffer, String channelId)
  {         
      try
      {
    	// Send Event to an Event Hub
        String message = new String(buffer.array(), StandardCharsets.UTF_8);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8); // "UTF_8"
    	iotMessage = new AWSIoTPublishListener(topicName, AWSIotQos.QOS0, bytes);
    	 
        if (isEventHubType)
        {                                      
          if (awsClient != null){
        	  awsClient.publish(iotMessage);
          }
          else
          {
            LOGGER.warn("FAILED_TO_SEND_INVALID_EH_CONNECTION", clientEndpoint);
          }                   
        }
        else
        {
          // Send Event to a Device - update shadow          
          String deviceId = deviceIdFieldName;
          if (deviceId != null & Validator.isNotBlank(deviceId))
          {     
        	geIoTDevice.delete(); // delete shadow
            geIoTDevice.update(iotMessage, 5000); // update device state
          }
          else
          {
            LOGGER.warn("FAILED_TO_SEND_INVALID_DEVICE_ID", deviceIdFieldName);
          }
        }
      }
      catch (Exception e)
      {
        // streamClient.stop();        
        LOGGER.error(e.getMessage(), e);
        setErrorMessage(e.getMessage());
        setRunningState(RunningState.ERROR);
      }    
  }

  /*
   * Non-blocking Publish Listener
   */
  public final class AWSIoTPublishListener extends AWSIotMessage {

	    public AWSIoTPublishListener(String topic, AWSIotQos qos, byte [] payload) {
	        super(topic, qos, payload);
	    }

	    @Override
	    public void onSuccess() {
	        System.out.println(System.currentTimeMillis() + ": >>> " + getStringPayload());
	    }

	    @Override
	    public void onFailure() {
	        System.out.println(System.currentTimeMillis() + ": publish failed for " + getStringPayload());
	    }

	    @Override
	    public void onTimeout() {
	        System.out.println(System.currentTimeMillis() + ": publish timeout for " + getStringPayload());
	    }

	}

}
