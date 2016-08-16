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
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.GeoEventAwareTransport;
import com.esri.ges.transport.OutboundTransportBase;
import com.esri.ges.transport.TransportDefinition;
import com.esri.ges.util.Validator;

public class AwsIoTHubOutboundTransport extends OutboundTransportBase implements GeoEventAwareTransport
{
  // logger
  private static final BundleLogger LOGGER                 = BundleLoggerFactory.getLogger(AwsIoTHubOutboundTransport.class);

  // connection properties
  private String                    iotServiceType         = "";  
  private String                    deviceIdGedName        = "";
  private String                    deviceIdFieldName      = "";
  
  private String 					clientEndpoint		   = "";
  private String				    rootCertificate		   = "";
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

  public AwsIoTHubOutboundTransport(TransportDefinition definition) throws ComponentException
  {
    super(definition);
  }

  @Override
  public synchronized void start()
  {
    switch (getRunningState())
    {
      case STARTING:
      case STARTED:
        return;
      default:
    }

    setRunningState(RunningState.STARTING);
    setup();
  }

  public void readProperties()
  {
    try
    {
      boolean somethingChanged = false;

      if (hasProperty(AwsIoTHubOutboundTransportDefinition.IOT_SERVICE_TYPE_PROPERTY_NAME))
      {
        // IoT Service Type
        String newIotServiceType = getProperty(AwsIoTHubOutboundTransportDefinition.IOT_SERVICE_TYPE_PROPERTY_NAME).getValueAsString();
        if (!iotServiceType.equals(newIotServiceType))
        {
          iotServiceType = newIotServiceType;
          somethingChanged = true;
        }
      }
      // Client End point
      if (hasProperty(AwsIoTHubOutboundTransportDefinition.CLIENT_ENDPOINT_PROPERTY_NAME))
      {
        String newClientEndpoint  = getProperty(AwsIoTHubOutboundTransportDefinition.CLIENT_ENDPOINT_PROPERTY_NAME).getValueAsString();
        if (!clientEndpoint.equals(newClientEndpoint))
        {
          clientEndpoint = newClientEndpoint;
          somethingChanged = true;
        }
      }
      //rootCA
      if (hasProperty(AwsIoTHubOutboundTransportDefinition.ROOT_CA_PROPERTY_NAME))
      {
        String newRootCA  = getProperty(AwsIoTHubOutboundTransportDefinition.ROOT_CA_PROPERTY_NAME).getValueAsString();
        if (!rootCertificate.equals(newRootCA))
        {
         rootCertificate = newRootCA;
         somethingChanged = true;
        }
      }      
      //X.509 certificate
      if (hasProperty(AwsIoTHubOutboundTransportDefinition.X509_CERTIFICATE_PROPERTY_NAME))
      {
        String newX509Cert  = getProperty(AwsIoTHubOutboundTransportDefinition.X509_CERTIFICATE_PROPERTY_NAME).getValueAsString();
        if (!x509Certificate.equals(newX509Cert))
        {
         x509Certificate = newX509Cert;
         somethingChanged = true;
        }
      }
     //private key
      if (hasProperty(AwsIoTHubOutboundTransportDefinition.PRIVATE_KEY_PROPERTY_NAME))
      {
        String newPrivateKey  = getProperty(AwsIoTHubOutboundTransportDefinition.PRIVATE_KEY_PROPERTY_NAME).getValueAsString();
        if (!privateKey.equals(newPrivateKey))
        {
         privateKey = newPrivateKey;
         somethingChanged = true;
        }
      }
      //topic name
      if (hasProperty(AwsIoTHubOutboundTransportDefinition.TOPIC_NAME_PROPERTY_NAME))
      {
        String newTopicName  = getProperty(AwsIoTHubOutboundTransportDefinition.TOPIC_NAME_PROPERTY_NAME).getValueAsString();
        if (!topicName.equals(newTopicName))
        {
         topicName = newTopicName;
         somethingChanged = true;
        }
      }     
      // Device Id GED Name
      if (hasProperty(AwsIoTHubOutboundTransportDefinition.DEVICE_ID_GED_NAME_PROPERTY_NAME))
      {
        String newGEDName = getProperty(AwsIoTHubOutboundTransportDefinition.DEVICE_ID_GED_NAME_PROPERTY_NAME).getValueAsString();
        if (!deviceIdGedName.equals(newGEDName))
        {
          deviceIdGedName = newGEDName;
          somethingChanged = true;
        }
      }
      // Device Id Field Name
      if (hasProperty(AwsIoTHubOutboundTransportDefinition.DEVICE_ID_FIELD_NAME_PROPERTY_NAME))
      {
        String newDeviceIdFieldName = getProperty(AwsIoTHubOutboundTransportDefinition.DEVICE_ID_FIELD_NAME_PROPERTY_NAME).getValueAsString();
        if (!deviceIdFieldName.equals(newDeviceIdFieldName))
        {
          deviceIdFieldName = newDeviceIdFieldName;
          somethingChanged = true;
        }
      }
      propertiesNeedUpdating = somethingChanged;
    }
    catch (Exception ex)
    {
      LOGGER.error("INIT_ERROR", ex.getMessage());
      LOGGER.info(ex.getMessage(), ex);
      setErrorMessage(ex.getMessage());
      setRunningState(RunningState.ERROR);
    }
  }

  public synchronized void setup()
  {
    String errorMessage = null;
    RunningState runningState = RunningState.STARTED;

    try
    {
      readProperties();
      if (propertiesNeedUpdating)
      {
        cleanup();
        propertiesNeedUpdating = false;
      }

      // setup
      isEventHubType = AwsIoTHubOutboundTransportDefinition.IOT_SERVICE_TYPE_EVENT_HUB.equals(iotServiceType);
      
      //AWS Event Hub       
      KeyStorePasswordPair pair = AwsIoTHubUtil.getKeyStorePasswordPair(x509Certificate, privateKey, null);
      awsClient = new AWSIotMqttClient(clientEndpoint, deviceIdGedName , pair.keyStore, pair.keyPassword);
      if (awsClient == null)
      {
        runningState = RunningState.ERROR;
        errorMessage = LOGGER.translate("FAILED_TO_CREATE_EH_CLIENT", clientEndpoint);
        LOGGER.error(errorMessage);
      }        
      
      if (!isEventHubType)
      {         	
        // IoT Device
    	geIoTDevice = new GEIoTDevice(deviceIdFieldName);
    	awsClient.attach(geIoTDevice);    	        
      }      
      awsClient.connect();      

      setErrorMessage(errorMessage);
      setRunningState(runningState);
    }
    catch (Exception ex)
    {
      LOGGER.error("INIT_ERROR", ex.getMessage());
      LOGGER.info(ex.getMessage(), ex);
      setErrorMessage(ex.getMessage());
      setRunningState(RunningState.ERROR);
    }
  }

  protected void cleanup()
  {
    // clean up the aws hub client       
	  if (awsClient != null)
      {
        try
        {
        	if (geIoTDevice !=null) {       
        		awsClient.detach(geIoTDevice);        		
        		geIoTDevice.delete();
        	}
        	awsClient.disconnect();        
        }
        catch (Exception e)
        {
          LOGGER.warn("CLEANUP_ERROR", e);
        }
        geIoTDevice = null;
        awsClient = null;
      }             
  }

  @Override
  public void receive(ByteBuffer buffer, String channelId)
  {
    receive(buffer, channelId, null);
  }

  @Override
  public void receive(ByteBuffer buffer, String channelId, GeoEvent geoEvent)
  {
    if (isRunning())
    {
      if (geoEvent == null)
        return;

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
          Object deviceIdObj = geoEvent.getField(deviceIdFieldName);
          String deviceId = "";
          if (deviceIdObj != null)
            deviceId = deviceIdObj.toString();

          if (Validator.isNotBlank(deviceId))
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
        setErrorMessage(e.getMessage());
        LOGGER.error(e.getMessage(), e);
        setRunningState(RunningState.ERROR);
      }
    }
    else
    {
      LOGGER.debug("RECEIVED_BUFFER_WHEN_STOPPED", "");
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
