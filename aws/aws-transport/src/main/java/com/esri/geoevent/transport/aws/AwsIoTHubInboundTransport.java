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

import com.amazonaws.services.iot.client.AWSIotMessage;
import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.AWSIotQos;
import com.amazonaws.services.iot.client.AWSIotTopic;
import com.esri.geoevent.transport.aws.AwsIoTHubUtil.KeyStorePasswordPair;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.InboundTransportBase;
import com.esri.ges.transport.TransportDefinition;

/*import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiveHandler;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.servicebus.ServiceBusException;*/

public class AwsIoTHubInboundTransport extends InboundTransportBase
{	
  //based on the aws-iot-device-sdk-java	
  //https://github.com/aws/aws-iot-device-sdk-java

  // logger
  private static final BundleLogger LOGGER                     = BundleLoggerFactory.getLogger(AwsIoTHubInboundTransport.class);
  
  // transport properties 
  private boolean                   isEventHubType         = true;
  private String                    iotServiceType         = "";
  
  private String                    deviceIdGedName        = "";
  private String                    deviceIdFieldName      = "";
  
  private String 					clientEndpoint		   = "";
  private String				    rootCertificate		   = "";
  private String					x509Certificate		   = "";
  private String 					privateKey			   = "";
  private String 					topicName			   = "";
  
  // data members
  private AWSIotMqttClient 			awsClient			    = null;
  private static GEIoTDevice 		geIoTDevice				 = null;
  
  private String                    errorMessage;
  
  private volatile boolean          propertiesNeedUpdating = false;
  

  public AwsIoTHubInboundTransport(TransportDefinition definition) throws ComponentException
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
      case ERROR:
        return;
      default:
    }
    setRunningState(RunningState.STARTING);
    setup();    
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
      isEventHubType = AwsIoTHubInboundTransportDefinition.IOT_SERVICE_TYPE_EVENT_HUB.equals(iotServiceType);
      
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
      
      AWSIotTopic topic = new AwsIoTTopicListener(topicName, AWSIotQos.QOS0);
      awsClient.subscribe(topic, true);

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

  @Override
  public synchronized void stop()
  {
    if (getRunningState() == RunningState.STOPPING)
      return;

    errorMessage = null;
    setRunningState(RunningState.STOPPING);
    cleanup();
    // setErrorMessage(null);
    setRunningState(RunningState.STOPPED);
  }

  protected void cleanup()
  {     
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
    LOGGER.debug("CLEANUP_COMPLETE");
  }

  @Override
  public void validate()
  {
    // TODO: Validate
  }

  @Override
  public synchronized boolean isRunning()
  {
    return (getRunningState() == RunningState.STARTED);
  }

  public void readProperties() throws Exception
  {
    try
    {
      boolean somethingChanged = false;
            
      //iot service type
      if (hasProperty(AwsIoTHubInboundTransportDefinition.IOT_SERVICE_TYPE_PROPERTY_NAME))
      {
        // IoT Service Type
        String newIotServiceType = getProperty(AwsIoTHubInboundTransportDefinition.IOT_SERVICE_TYPE_PROPERTY_NAME).getValueAsString();
        if (!iotServiceType.equals(newIotServiceType))
        {
          iotServiceType = newIotServiceType;
          somethingChanged = true;
        }
      }      
      // Client End point
      if (hasProperty(AwsIoTHubInboundTransportDefinition.CLIENT_ENDPOINT_PROPERTY_NAME))
      {
        String newClientEndpoint  = getProperty(AwsIoTHubInboundTransportDefinition.CLIENT_ENDPOINT_PROPERTY_NAME).getValueAsString();
        if (!clientEndpoint.equals(newClientEndpoint))
        {
          clientEndpoint = newClientEndpoint;
          somethingChanged = true;
        }
      }
      //rootCA
      if (hasProperty(AwsIoTHubInboundTransportDefinition.ROOT_CA_PROPERTY_NAME))
      {
        String newRootCA  = getProperty(AwsIoTHubInboundTransportDefinition.ROOT_CA_PROPERTY_NAME).getValueAsString();
        if (!rootCertificate.equals(newRootCA))
        {
         rootCertificate = newRootCA;
         somethingChanged = true;
        }
      }      
      //X.509 certificate
      if (hasProperty(AwsIoTHubInboundTransportDefinition.X509_CERTIFICATE_PROPERTY_NAME))
      {
        String newX509Cert  = getProperty(AwsIoTHubInboundTransportDefinition.X509_CERTIFICATE_PROPERTY_NAME).getValueAsString();
        if (!x509Certificate.equals(newX509Cert))
        {
         x509Certificate = newX509Cert;
         somethingChanged = true;
        }
      }
     //private key
      if (hasProperty(AwsIoTHubInboundTransportDefinition.PRIVATE_KEY_PROPERTY_NAME))
      {
        String newPrivateKey  = getProperty(AwsIoTHubInboundTransportDefinition.PRIVATE_KEY_PROPERTY_NAME).getValueAsString();
        if (!privateKey.equals(newPrivateKey))
        {
         privateKey = newPrivateKey;
         somethingChanged = true;
        }
      }
      //topic name
      if (hasProperty(AwsIoTHubInboundTransportDefinition.TOPIC_NAME_PROPERTY_NAME))
      {
        String newTopicName  = getProperty(AwsIoTHubInboundTransportDefinition.TOPIC_NAME_PROPERTY_NAME).getValueAsString();
        if (!topicName.equals(newTopicName))
        {
         topicName = newTopicName;
         somethingChanged = true;
        }
      }
      // Device Id GED Name
      if (hasProperty(AwsIoTHubInboundTransportDefinition.DEVICE_ID_GED_NAME_PROPERTY_NAME))
      {
        String newGEDName = getProperty(AwsIoTHubInboundTransportDefinition.DEVICE_ID_GED_NAME_PROPERTY_NAME).getValueAsString();
        if (!deviceIdGedName.equals(newGEDName))
        {
          deviceIdGedName = newGEDName;
          somethingChanged = true;
        }
      }
      // Device Id Field Name
      if (hasProperty(AwsIoTHubInboundTransportDefinition.DEVICE_ID_FIELD_NAME_PROPERTY_NAME))
      {
        String newDeviceIdFieldName = getProperty(AwsIoTHubInboundTransportDefinition.DEVICE_ID_FIELD_NAME_PROPERTY_NAME).getValueAsString();
        if (!deviceIdFieldName.equals(newDeviceIdFieldName))
        {
          deviceIdFieldName = newDeviceIdFieldName;
          somethingChanged = true;
        }
      }
      
      propertiesNeedUpdating = somethingChanged;
    }
    catch (Exception e)
    {
      errorMessage = LOGGER.translate("ERROR_READING_PROPS");
      LOGGER.error("ERROR_READING_PROPS", e);
    }
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
