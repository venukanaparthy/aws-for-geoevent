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

import java.util.ArrayList;
import java.util.List;

import com.esri.ges.core.property.LabeledValue;
import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.TransportDefinitionBase;
import com.esri.ges.transport.TransportType;

public class AwsIoTHubOutboundTransportDefinition extends TransportDefinitionBase
{
  // logger
  private static final BundleLogger LOGGER                             = BundleLoggerFactory.getLogger(AwsIoTHubOutboundTransportDefinition.class);

  // property names
  public static final String        IOT_SERVICE_TYPE_PROPERTY_NAME     = "iotServiceType";  
  public static final String        DEVICE_ID_GED_NAME_PROPERTY_NAME   = "deviceIdGedName";
  public static final String        DEVICE_ID_FIELD_NAME_PROPERTY_NAME = "deviceIdFieldName";

  public static final String        IOT_SERVICE_TYPE_EVENT_HUB         = "Event Hub";
  public static final String        IOT_SERVICE_TYPE_IOT_DEVICE        = "IoT Device";

  public static final String        IOT_SERVICE_TYPE_DEFAULT_VALUE     = IOT_SERVICE_TYPE_EVENT_HUB;
  
  public static final String   		CLIENT_ENDPOINT_PROPERTY_NAME  	   = "clientEndPoint";
  public static final String 		ROOT_CA_PROPERTY_NAME			   = "rootCertificateAuthority";
  public static final String		X509_CERTIFICATE_PROPERTY_NAME	   = "x509Certificate";	
  public static final String		PRIVATE_KEY_PROPERTY_NAME	 	   = "privateKey";
  public static final String		TOPIC_NAME_PROPERTY_NAME		   = "topicName";	
  
  public AwsIoTHubOutboundTransportDefinition()
  {
    super(TransportType.OUTBOUND);
    try
    {
      String dependsOnNone = null;
      List<LabeledValue> iotServiceTypesAllowedValues = new ArrayList<LabeledValue>();
      iotServiceTypesAllowedValues.add(new LabeledValue("${com.esri.geoevent.transport.aws-transport.IOT_SERVICE_TYPE_EVENT_HUB_LBL}", IOT_SERVICE_TYPE_EVENT_HUB));
      iotServiceTypesAllowedValues.add(new LabeledValue("${com.esri.geoevent.transport.aws-transport.IOT_SERVICE_TYPE_IOT_DEVICE_LBL}", IOT_SERVICE_TYPE_IOT_DEVICE));
      
      propertyDefinitions.put(IOT_SERVICE_TYPE_PROPERTY_NAME, new PropertyDefinition(IOT_SERVICE_TYPE_PROPERTY_NAME, PropertyType.String, IOT_SERVICE_TYPE_DEFAULT_VALUE, "${com.esri.geoevent.transport.aws-transport.IOT_SERVICE_TYPE_LBL}", "${com.esri.geoevent.transport.aws-transport.IOT_SERVICE_TYPE_DESC}", dependsOnNone, true, false, iotServiceTypesAllowedValues));
      propertyDefinitions.put(CLIENT_ENDPOINT_PROPERTY_NAME, new PropertyDefinition(CLIENT_ENDPOINT_PROPERTY_NAME, PropertyType.String, null, "${com.esri.geoevent.transport.aws-transport.CLIENT_ENDPOINT_LBL}", "${com.esri.geoevent.transport.aws-transport.CLIENT_ENDPOINT_DESC}", true, false));      
      propertyDefinitions.put(ROOT_CA_PROPERTY_NAME, new PropertyDefinition(ROOT_CA_PROPERTY_NAME, PropertyType.String, null, "${com.esri.geoevent.transport.aws-transport.ROOT_CA_LBL}", "${com.esri.geoevent.transport.aws-transport.ROOT_CA_DESC}", true, false));
      propertyDefinitions.put(X509_CERTIFICATE_PROPERTY_NAME, new PropertyDefinition(X509_CERTIFICATE_PROPERTY_NAME, PropertyType.String, null, "${com.esri.geoevent.transport.aws-transport.CERTIFICATE_LBL}", "${com.esri.geoevent.transport.aws-transport.CERTIFICATE_DESC}", true, false));      
      propertyDefinitions.put(PRIVATE_KEY_PROPERTY_NAME, new PropertyDefinition(PRIVATE_KEY_PROPERTY_NAME, PropertyType.String, null, "${com.esri.geoevent.transport.aws-transport.PRIVATE_KEY_LBL}", "${com.esri.geoevent.transport.aws-transport.PRIVATE_KEY_LBL_DESC}", true, false));
      propertyDefinitions.put(TOPIC_NAME_PROPERTY_NAME, new PropertyDefinition(TOPIC_NAME_PROPERTY_NAME, PropertyType.String, null, "${com.esri.geoevent.transport.aws-transport.TOPIC_NAME_LBL}", "${com.esri.geoevent.transport.aws-transport.TOPIC_NAME_LBL_DESC}", true, false));
            //
      propertyDefinitions.put(DEVICE_ID_GED_NAME_PROPERTY_NAME, new PropertyDefinition(DEVICE_ID_GED_NAME_PROPERTY_NAME, PropertyType.GeoEventDefinition, null, "${com.esri.geoevent.transport.aws-transport.DEVICE_ID_GED_NAME_LBL}", "${com.esri.geoevent.transport.aws-transport.DEVICE_ID_GED_NAME_DESC}", "iotServiceType=IoT Device", true, false));
      propertyDefinitions.put(DEVICE_ID_FIELD_NAME_PROPERTY_NAME, new PropertyDefinition(DEVICE_ID_FIELD_NAME_PROPERTY_NAME, PropertyType.GeoEventDefinitionField, null, "${com.esri.geoevent.transport.aws-transport.DEVICE_ID_FIELD_NAME_LBL}", "${com.esri.geoevent.transport.aws-transport.DEVICE_ID_FIELD_NAME_DESC}", "iotServiceType=IoT Device", true, false));
    }
    catch (PropertyException error)
    {
      LOGGER.error("ERROR_LOADING_TRANSPORT_DEFINITION", error);
      throw new RuntimeException(error);
    }
  }

  @Override
  public String getName()
  {
    return "AwsIoTHub";
  }

  @Override
  public String getLabel()
  {
    return "${com.esri.geoevent.transport.aws-transport.TRANSPORT_OUT_LABEL}";
  }

  @Override
  public String getDomain()
  {
    return "com.esri.geoevent.transport.outbound";
  }

  @Override
  public String getDescription()
  {
    return "${com.esri.geoevent.transport.aws-transport.TRANSPORT_OUT_DESC}";
  }
}
