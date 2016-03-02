package bolt.packets;

public class DummyControlPacket extends ControlPacket{
	
	public DummyControlPacket(){
		
	}
	
	@Override
	public byte[]encodeControlInformation(){
		return null;
	}
}