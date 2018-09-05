package jedisUntils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.ScanParams;
import redis.clients.jedis.Protocol.Keyword;

public class ScanOptions extends ScanParams{



	 private final Map<Keyword, ByteBuffer> params = new EnumMap<Keyword, ByteBuffer>(Keyword.class);

	@Override
	public Collection<byte[]> getParams() {
		  List<byte[]> paramsList = new ArrayList<byte[]>(params.size());
		    for (Map.Entry<Keyword, ByteBuffer> param : params.entrySet()) {
		      paramsList.add(param.getKey().raw);
		      paramsList.add(param.getValue().array());
		    }
		    return Collections.unmodifiableCollection(paramsList);
	}
	
}
