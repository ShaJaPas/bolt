package bolt.performance;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import bolt.BoltReceiver;
import bolt.BoltSession;
import bolt.cc.SimpleTCP;

//uses different CC algorithm
public class TestBoltLargeDataCC1 extends TestBoltLargeData {
	
	boolean running=false;

	//how many
	int num_packets=100;
	
	//how large is a single packet
	int size=1*1024*1024;
	
	int TIMEOUT=Integer.MAX_VALUE;
	
	int READ_BUFFERSIZE=1*1024*1024;

	@Override
	@Test
	public void test1()throws Exception{
		Logger.getLogger("bolt").setLevel(Level.INFO);
		BoltReceiver.dropRate=0;
		System.setProperty(BoltSession.CC_CLASS, SimpleTCP.class.getName());
		TIMEOUT=Integer.MAX_VALUE;
		doTest();
	}

}
