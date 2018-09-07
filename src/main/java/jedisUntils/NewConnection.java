package jedisUntils;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Protocol.Command;

public class NewConnection  extends Connection{
	public NewConnection() {
		super("140.143.161.103", 6379);
	}
	
	
	@Override
	protected Connection sendCommand(Command cmd, String... args) {
		return super.sendCommand(cmd, args);
	}


	@Override
	protected Connection sendCommand(Command cmd, byte[]... args) {
		// TODO Auto-generated method stub
		return super.sendCommand(cmd, args);
	}
	
	


	
}
