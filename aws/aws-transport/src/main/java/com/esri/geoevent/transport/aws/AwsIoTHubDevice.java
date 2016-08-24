package com.esri.geoevent.transport.aws;

import java.util.Random;

import com.amazonaws.services.iot.client.AWSIotDevice;
import com.amazonaws.services.iot.client.AWSIotDeviceProperty;

/**
 * This class encapsulates an actual device. It extends {@link AWSIotDevice} to
 * define properties that are to be kept in sync with the AWS IoT shadow.
 */
public class AwsIoTHubDevice extends AWSIotDevice {

	 private enum MyBulbStatus {
		  ON(0),
		  OFF(1),
		  BLINK(2),
		  UNKNOWN(3);
		  private int _state;
		  
		  private MyBulbStatus(int s){
			_state = s;  
		  }
		  
		  int getState(){
			  return _state;
		  }
	   }

	public AwsIoTHubDevice(String thingName) {
		super(thingName);	
	}
	
	  @AWSIotDeviceProperty
	  private String bulbState;
	  
	  public String getBulbState() {		  		
	  	//get bulb state from gpio; 
	  	//get a random state
	  	String _defaultRet = MyBulbStatus.UNKNOWN.toString();
	   	int _state  = new Random().nextInt(4);
	   	for (MyBulbStatus s : MyBulbStatus.values()) {
	        if (s.getState() == _state) {
	          	_defaultRet = s.toString();
	           	break;
	         }
	    }
	    System.out.println(System.currentTimeMillis() + " >>> reported bulb state: " + (_defaultRet));
	    
	    return _defaultRet;
	  }
	  
	  public void setBulbState(String desiredState) {
		  System.out.println(System.currentTimeMillis() + " <<< desired bulb state to " + (desiredState));
		  this.bulbState = MyBulbStatus.valueOf(desiredState).toString();
	  }
}
