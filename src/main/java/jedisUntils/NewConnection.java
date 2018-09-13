package jedisUntils;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Protocol.Command;

public class NewConnection  extends Connection{
	public NewConnection() {
		super(FileUntil.FileUntil.getStringProperties(JedisUntils.PATH, "ip"), FileUntil.FileUntil.getIntProperties(JedisUntils.PATH, "port"));
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
