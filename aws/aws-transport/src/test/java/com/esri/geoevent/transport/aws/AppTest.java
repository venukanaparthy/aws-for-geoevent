package com.esri.geoevent.transport.aws;

import com.amazonaws.services.iot.client.AWSIotMessage;
import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.esri.geoevent.transport.aws.AwsIoTHubUtil.KeyStorePasswordPair;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
	
	/*private AWSIotMqttClient _mqttClient 		= null;
	private AWSIotMessage _awsIotMessage 		= null;
	private KeyStorePasswordPair _ksp 			= null;*/
	
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }
    
    @Override
    protected void setUp(){    	
    }
    
    protected void tearDown(){
    	
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }
}
