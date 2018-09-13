package FileUntil;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class FileUntil {

	/**
	 * ����key��ȡvalue
	 * 
	 * @Title: ʹ�û�����������ȡ�����ļ���Ȼ������أ��ٰ������
	 *         ����·�������·���� ��������·������ӵ�ǰ��Ŀ�µ�Ŀ¼��ʼ���㣬
	 *         �磺��ǰ��Ŀ·��/config/config.properties, ���·������config/config.properties
	 * 
	 * @param filePath
	 * @param keyWord
	 * @return @return String @throws
	 */
	public static String getStringProperties(String filePath, String keyWord) {
		Properties prop = new Properties();
		String value = null;
		try {
			// ͨ�����뻺�������ж�ȡ�����ļ�
			InputStream InputStream = new BufferedInputStream(new FileInputStream(new File(filePath)));
			// ����������
			prop.load(InputStream);
			// ���ݹؼ��ֻ�ȡvalueֵ
			value = prop.getProperty(keyWord);
			if(value == null){
				throw new NullPointerException();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}
	
	public static Integer getIntProperties(String filePath, String keyWord) {
		Properties prop = new Properties();
		String value = null;
		try {
			// ͨ�����뻺�������ж�ȡ�����ļ�
			InputStream InputStream = new BufferedInputStream(new FileInputStream(new File(filePath)));
			// ����������
			prop.load(InputStream);
			// ���ݹؼ��ֻ�ȡvalueֵ
			value = prop.getProperty(keyWord);
			if(value == null){
				throw new NullPointerException();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return Integer.valueOf(value);
	}
	
	public static Boolean getBooleanProperties(String filePath, String keyWord) {
		Properties prop = new Properties();
		String value = null;
		try {
			// ͨ�����뻺�������ж�ȡ�����ļ�
			InputStream InputStream = new BufferedInputStream(new FileInputStream(new File(filePath)));
			// ����������
			prop.load(InputStream);
			// ���ݹؼ��ֻ�ȡvalueֵ
			value = prop.getProperty(keyWord);
			if(value == null){
				throw new NullPointerException();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return Boolean.valueOf(value);
	}

}
