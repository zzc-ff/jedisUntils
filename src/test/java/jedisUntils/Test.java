package jedisUntils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Test {

	@org.junit.Test
	public void testClusterNodes() {
		JedisUntils ju = new JedisUntils();
		String[] nodesId = ju.clusterNodes();
		for (String node : nodesId) {
			System.out.println(node);
		}
	}

	@org.junit.Test
	public void testPipeline() {
		JedisUntils j = new JedisUntils();
		List<String> list = new LinkedList<String>();
		list.add("zzc");
		list.add("zcc");
		list.add("ccc");
		Map<String, String> map = j.pipelineWithGet(list);
		for (String entry : map.keySet()) {
			System.out.println(entry + ":" + map.get(entry));
		}
	}

	@org.junit.Test
	public void testpipeline() {
		JedisUntils j = new JedisUntils();
		Map<String, String> map = new HashMap<String, String>();
		map.put("zzc", "zzc");
		map.put("zcc", "zcc");
		map.put("ccc", "ccc");
		List<Object> list = j.pipelineWithSet(map);
		for (Object o : list) {
			System.out.println(String.valueOf(o));
		}
	}

	@org.junit.Test
	public void testscan() {
		JedisUntils j = new JedisUntils();
		Map<String ,List<Object>> map = j.scans();
		Iterator<String> it = map.keySet().iterator();
		while(it.hasNext()){
			System.out.println(it.next()+":"+map.get(it.next()).size());
		}
	}
	
	@org.junit.Test
	public void testKeys() {
		JedisUntils ju = new JedisUntils();
		Map<String ,List<String>> map = ju.keys("*");
		Iterator<String> it = map.keySet().iterator();
		while(it.hasNext()){
			System.out.println(it.next()+":"+map.get(it.next()));
		}
	}

	
	@org.junit.Test
	public void testscanWithPattern() {
		JedisUntils j = new JedisUntils();
		Map<String ,List<Object>> map = j.scans("AP:*");
		Iterator<String> it = map.keySet().iterator();
		while(it.hasNext()){
			System.out.println(it.next()+":"+map.get(it.next()).size());
		}
	}
	
	@org.junit.Test
	public void testscanWithCount() {
		JedisUntils j = new JedisUntils();
		Map<String ,List<Object>> map = j.scans(8);
		Iterator<String> it = map.keySet().iterator();
		while(it.hasNext()){
			System.out.println(it.next()+":"+map.get(it.next()).size());
		}
	}
	
	@org.junit.Test
	public void testscanWithPatternCount() {
		JedisUntils j = new JedisUntils();
		Map<String ,List<Object>> map = j.scans("AP:*",8);
		Iterator<String> it = map.keySet().iterator();
		while(it.hasNext()){
			System.out.println(it.next()+":"+map.get(it.next()).size());
		}
	}
	
	@org.junit.Test
	public void testget() {
		JedisUntils j = new JedisUntils();
		System.out.println(j.get("ccc"));
	}
	
	@org.junit.Test
	public void testFileUntil(){
		String path = FileUntil.FileUntil.getStringProperties("src/main/resources/jedis.properties", "path");
		System.out.println(path);
	}
	
	@org.junit.Test
	public void test(){
		JedisUntils j = new JedisUntils();
		j.get("zzc");
	}
}
