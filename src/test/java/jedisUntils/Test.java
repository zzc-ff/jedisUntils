package jedisUntils;

import java.util.Iterator;
import java.util.Set;

public class Test {
	@org.junit.Test
	public void testClusterNodes(){
		JedisUntils ju = new JedisUntils();
		String[] nodesId = ju.clusterNodes();
		for(String node : nodesId ){
			System.out.println(node);
		}
	}
	
	@org.junit.Test
	public void testKeys(){
		JedisUntils ju = new JedisUntils();
		Set<String> set  = ju.keys("*");
		Iterator<String> it = set.iterator();
		while (it.hasNext()) {
			System.out.println(it.next());
		}
	}
	@org.junit.Test
	public void testKeyss(){
		JedisUntils j = new JedisUntils();
		System.out.println(j.keys("*"));
	}
	
}
