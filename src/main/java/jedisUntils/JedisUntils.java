package jedisUntils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sun.org.apache.xerces.internal.impl.xs.SchemaGrammar.Schema4Annotations;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

public class JedisUntils {
	private JedisPool jedispool;
	private Jedis jedis;
	//Redis������IP
    private static final String IP = "140.143.161.103";
   
    //Redis�Ķ˿ں�
    private static final int PORT = 6379;
  
    //jedispool�����ȵ�ʱ��
    private static final int TIMEOUT = 10000;
   
    //��������
    private static final String AUTH = "54djePZW6MUb";
    
    //���Ӻľ�ʱ�Ƿ�����, false���쳣,ture����ֱ����ʱ, Ĭ��true
    private static final boolean IS_BLOCK = true;

    //��������ʵ���������Ŀ��Ĭ��ֵΪ8��
    //�����ֵΪ-1�����ʾ�����ƣ����pool�Ѿ�������maxActive��jedisʵ�������ʱpool��״̬Ϊexhausted(�ľ�)��
    private static final int MAX_ACTIVE = 1024;

    //����һ��pool����ж��ٸ�״̬Ϊidle(���е�)��jedisʵ����Ĭ��ֵҲ��8��
    private static final int MAX_IDLE = 200;

    //�ȴ��������ӵ����ʱ�䣬��λ���룬Ĭ��ֵΪ-1����ʾ������ʱ����������ȴ�ʱ�䣬��ֱ���׳�JedisConnectionException��
    private static final int MAX_WAIT = 10000;
    
    //������ӵ���С����ʱ�� Ĭ��1800000����(30����)
    private static final int MIN_EVICTABLEIDLE = 180000;

    //��borrowһ��jedisʵ��ʱ���Ƿ���ǰ����validate���������Ϊtrue����õ���jedisʵ�����ǿ��õģ�
    private static final boolean TEST_ON_BORROW = true;
  
    //��returnһ��jedisʵ��ʱ���Ƿ���ǰ����validate���������Ϊtrue����õ���jedisʵ�����ǿ��õģ�
    private static final boolean TEST_ON_RETURN = true;

/**
 * ��ʼ�����ݾݿ����ӳ�
 * ���������Ϣ����
 */
	{
		JedisPoolConfig config = new JedisPoolConfig();
		config.setBlockWhenExhausted(IS_BLOCK);
		config.setMaxTotal(MAX_ACTIVE);
		config.setMaxIdle(MAX_IDLE);
		config.setMaxWaitMillis(MAX_WAIT);
		config.setMinEvictableIdleTimeMillis(MIN_EVICTABLEIDLE);
		config.setTestOnBorrow(TEST_ON_BORROW);
		config.setTestOnReturn(TEST_ON_RETURN);
		jedispool = new JedisPool(config, IP, PORT, TIMEOUT, AUTH);
		System.out.println("jedispool���ӳس�ʼ��������");
	}
	
	/**
	 * ��ȡ������
	 */
	public JedisPool getJedispool() {
		return jedispool;
	}
	
	/**
	 * 
	 * @return jedispool.getResource() ����redis���ӳ����ó���jedis����
	 * 
	 * @return null ��ȡ��Դ�쳣������null
	 */
	public Jedis getResource() {
		System.out.println("jedis ��ȡ���ӡ�����");
		try {
			return jedispool.getResource();
		} catch (Exception e) {
			System.out.println("jedispool��ȡ��Դ���ӳ���");
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * �ر�jedis
	 * @param jedis
	 */
	public void jedisClose(Jedis jedis) {
		System.out.println("jedis �ر����ӡ�����");
		jedis.close();
	}
	
	/**
	 * �ر�jedispool
	 * @param jedispool
	 */
	public void poolClose(JedisPool jedispool) {
		System.out.println("jedispool �ر����ӳء�������");
		jedispool.close();
	}
	
	/**��clusterNodes�����ڵ���Ϣ,��ȡ��nodesId
	 * 
	 * @return �ַ����� ����ȡ��nodesId���������з���
	 */
	public String[] clusterNodes(){
		int start = 0;
		int NodesIdLength = 40;
		jedis = getResource();
		String nodes = jedis.clusterNodes();
		String[] nodesIdTemp = nodes.split("\n");
		String[] nodesId = new String[nodesIdTemp.length];
		for(int i =0;i<nodesIdTemp.length;i++){
			nodesId[i] = nodesIdTemp[i].substring(start,NodesIdLength);
		}
		jedisClose(jedis);
		return nodesId;
	}
	
	public String[] pipelineWithSet(Map<String,String> map){
		jedis =getResource();
		Pipeline p  = jedis.pipelined();
		Iterator it = map.entrySet().iterator();
		  while (it.hasNext()) {
		   Map.Entry entry = (Map.Entry) it.next();
		   String key = (String) entry.getKey();
		   String value = (String) entry.getValue();
		   p.set(key, value);
		  }
		
		return null;
	}
	
	
	
	/**keys������b��
	 * 
	 * @param pattern
	 * @return
	 */
	
	public Set<String> keys(String pattern){
		jedis = getResource();
		StringBuffer strbuf = new StringBuffer();
		String[] nodesId = clusterNodes();
		ScanParams scanParams = new ScanParams();
		for(int i=0;i<nodesId.length;i++){
			strbuf.append(pattern+'\b'+nodesId[i]);
		}
		byte[] patter = new byte[6];
		jedis.keys(patter);
		
		return null;
	}
	
	/**
	 * ģ��ƥ��
	 * @param pattern key��������ʽ
	 * @param count ÿ��ɨ���������¼��ֵԽ�����ĵ�ʱ��Խ�̣�����Ӱ��redis���ܡ�������Ϊһǧ��һ��
	 * @return ƥ���key����
	    */
	public  List<String> scan(String pattern, int count){
	   List<String> list = new ArrayList<String>();

	   jedis = getResource();
	   if (jedis == null){
	      return list;
	   }

	      String cursor = ScanParams.SCAN_POINTER_START;

	      ScanOptions scanParams = new ScanOptions();
	     
	      scanParams.count(count);
	      scanParams.match(pattern);

	      do {
	         ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
	         list.addAll(scanResult.getResult());
	         cursor = scanResult.getStringCursor();
	      }while(!"0".equals(cursor));
	      
	      jedisClose(jedis);
	   return list;
	   
	}
	
}
