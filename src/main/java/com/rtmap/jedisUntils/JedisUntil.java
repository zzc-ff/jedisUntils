package jedisUntils;

import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * JedisUntil工具Bean
 *
 * @author zhangzicheng@rtmap.com
 * @project JedisUntils
 * @package jedisUntils
 * @date 9/13/2018
 */
public class JedisUntil {

	private static Logger logger = Logger.getLogger(JedisUntils.class);

	public final static String SCAN_POINTER_START = String.valueOf(0);
	public final static String PATH = "src/main/resources/jedis.properties";

	// Redis服务器IP
	private static final String IP = FileUntil.FileUntil.getStringProperties(PATH, "ip");
	// Redis的端口号
	private static final int PORT = FileUntil.FileUntil.getIntProperties(PATH, "port");
	// jedispool阻塞等地时间
	private static final int TIMEOUT = FileUntil.FileUntil.getIntProperties(PATH, "timeOut");
	// 访问密码
	private static final String AUTH = FileUntil.FileUntil.getStringProperties(PATH, "auth");
	// 连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
	private static final boolean IS_BLOCK = FileUntil.FileUntil.getBooleanProperties(PATH, "isBlock");
	// 可用连接实例的最大数目，默认值为8；
	// 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
	private static final int MAX_ACTIVE = FileUntil.FileUntil.getIntProperties(PATH, "maxActive");
	// 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
	private static final int MAX_IDLE = FileUntil.FileUntil.getIntProperties(PATH, "maxIdle");
	// 等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
	private static final int MAX_WAIT = FileUntil.FileUntil.getIntProperties(PATH, "maxWait");
	// 逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
	private static final int MIN_EVICTABLEIDLE = FileUntil.FileUntil.getIntProperties(PATH, "minEvictableIdle");
	// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
	private static final boolean TEST_ON_BORROW = FileUntil.FileUntil.getBooleanProperties(PATH, "testOnBrrow");
	// 在return一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
	private static final boolean TEST_ON_RETURN = FileUntil.FileUntil.getBooleanProperties(PATH, "testOnReturn");

	private static JedisPool jedisPool = null;
	NewConnection conn = null;

	/**
	 * 初始化Redis连接池
	 */
	static {
		PropertyConfigurator.configure(FileUntil.FileUntil.getStringProperties(PATH, "path"));
		JedisPoolConfig config = new JedisPoolConfig();
		config.setBlockWhenExhausted(IS_BLOCK);
		config.setMaxTotal(MAX_ACTIVE);
		config.setMaxIdle(MAX_IDLE);
		config.setMaxWaitMillis(MAX_WAIT);
		config.setMinEvictableIdleTimeMillis(MIN_EVICTABLEIDLE);
		config.setTestOnBorrow(TEST_ON_BORROW);
		config.setTestOnReturn(TEST_ON_RETURN);
		jedisPool = new JedisPool(config, IP, PORT, TIMEOUT, AUTH);
		logger.info("jedispool连接池初始化。。。");
	}

	interface Make<E> {
		public E execute(Jedis jedis);
	}

	public <E> E execute(Make<E> executor) {
		E result;
		Jedis jedis = jedisPool.getResource();
		try {
			result = executor.execute(jedis);
		} finally {
			jedis.close();
		}
		return result;
	}

	/**
	 * key相关的操作
	 *
	 * @param key
	 *            键
	 * @param value
	 *            值
	 * @return 返回操作后的结果
	 */
	Long del(String key) {
		return execute(jedis -> jedis.del(key));
	}

	Boolean exists(String key) {
		return execute(jedis -> jedis.exists(key));
	}

	Long expire(String key, int seconds) {
		return execute(jedis -> jedis.expire(key, seconds));
	}

	Long expireAt(String key, long unixTime) {
		return execute(jedis -> jedis.expireAt(key, unixTime));
	}

	String type(String key) {
		return execute(jedis -> jedis.type(key));
	}

	Long ttl(String key) {
		return execute(jedis -> jedis.ttl(key));
	}

	Long persist(String key) {
		return execute(jedis -> jedis.persist(key));
	}

	Long pexpireAt(String key, long millisecondsTimestamp) {
		return execute(jedis -> jedis.expireAt(key, millisecondsTimestamp));
	}

	Long pttl(String key) {
		return execute(jedis -> jedis.pttl(key));
	}

	String rename(String oldkey, String newkey) {
		return execute(jedis -> jedis.rename(oldkey, newkey));
	}

	Long renamenx(String oldkey, String newkey) {
		return execute(jedis -> jedis.renamenx(oldkey, newkey));
	}

	List<String> sort(String key) {
		return execute(jedis -> jedis.sort(key));
	}

	/**
	 * String 相关操作
	 *
	 * @param key
	 *            键
	 * @param value
	 *            值
	 * @return 返回操作后的结果
	 */
	String set(final String key, final String value) {
		return execute(jedis -> jedis.set(key, value));
	}

	String get(final String key) {
		return execute(jedis -> jedis.get(key));
	}

	String getSet(final String key, final String value) {
		return execute(jedis -> jedis.getSet(key, value));
	}

	Long decr(final String key) {
		return execute(jedis -> jedis.decr(key));
	}

	Long incrBy(final String key, final long integer) {
		return execute(jedis -> jedis.incrBy(key, integer));
	}

	Double incrByFloat(final String key, final double value) {
		return execute(jedis -> jedis.incrByFloat(key, value));
	}

	Long incr(final String key) {
		return execute(jedis -> jedis.incr(key));
	}

	Long append(final String key, final String value) {
		return execute(jedis -> jedis.append(key, value));
	}

	String substr(final String key, final int start, final int end) {
		return execute(jedis -> jedis.substr(key, start, end));
	}

	/**
	 * 推送元素到集合List中
	 *
	 * @param key
	 *            键
	 * @param value
	 *            值
	 * @return 推送后集合内元素个数
	 */
	public Long lpush(String key, String... value) {
		return execute(jedis -> jedis.lpush(key, value));
	}

	public Long rpush(String key, String... value) {
		return execute(jedis -> jedis.rpush(key, value));
	}

	public String lpop(String key) {
		return execute(jedis -> jedis.lpop(key));
	}

	public String rpop(String key) {
		return execute(jedis -> jedis.rpop(key));
	}

	/**
	 * set 相关操作
	 *
	 * @param key
	 *            键
	 * @param value
	 *            值
	 * @return 返回操作后的结果
	 */
	Long sadd(final String key, final String... members) {
		return execute(jedis -> jedis.sadd(key, members));
	}

	Set<String> smembers(final String key) {
		return execute(jedis -> jedis.smembers(key));
	}

	Long srem(final String key, final String... member) {
		return execute(jedis -> jedis.srem(key, member));
	}

	String spop(final String key) {
		return execute(jedis -> jedis.spop(key));
	}

	Set<String> spop(final String key, final long count) {
		return execute(jedis -> jedis.spop(key, count));
	}

	Long smove(final String srckey, final String dstkey, final String member) {
		return execute(jedis -> jedis.smove(srckey, dstkey, member));
	}

	Long scard(final String key) {
		return execute(jedis -> jedis.scard(key));
	}

	Boolean sismember(final String key, final String member) {
		return execute(jedis -> jedis.sismember(key, member));
	}

	/**
	 * zset 相关操作
	 *
	 * @param key
	 *            键
	 * @param value
	 *            值
	 * @return 返回操作后的结果
	 */

	/**
	 * hash 相关操作
	 *
	 * @param key
	 *            键
	 * @param value
	 *            值
	 * @return 返回操作后的结果
	 */

}