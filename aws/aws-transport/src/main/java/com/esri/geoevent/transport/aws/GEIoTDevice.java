package com.esri.geoevent.transport.aws;

import com.amazonaws.services.iot.client.AWSIotDevice;

public class GEIoTDevice extends AWSIotDevice {

	public GEIoTDevice(String thingName) {
		super(thingName);	
	}
}
