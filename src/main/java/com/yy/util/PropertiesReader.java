package com.yy.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * 专为加载配置文件所用
 * */
public class PropertiesReader {
	private static Logger msgLogInfo = Logger.getLogger("msgInfo");
	private static String PROPFILE = "config.properties";
	private static Properties properties = new Properties();
	static {
		loadFile();
	}

	/**
	 * 加载配置文件
	 */
	private static void loadFile() {
		String configPath = PathUtil.getConfigPath(PROPFILE);
		if (configPath == null) {
			msgLogInfo.error("无法加载配置文件config.properties路径，请检查该配置文件路径是否正确！");
		} else {
			try {
				//msgLogInfo.debug(PROPFILE + "路径：" + configPath);
				InputStream file = new FileInputStream(new File(configPath));	
				properties.load(file);
				file.close();
			} catch (Exception e) {
				msgLogInfo.error("加载配置文件config.properties出现异常!");
				e.printStackTrace();
			}
		}
	}

	/**
	 * 读取配置信息（支持重新加载配置文件）
	 * 
	 * @param name
	 * @return
	 */
	public static String getProperty4Reload(String name) {
		String value = properties.getProperty(name);
		if (value == null) {
			loadFile();
			value = properties.getProperty(name);
			if (value == null) {
				msgLogInfo.warn("文件config.properties在重新读取后，获取key:" + name+ "的值依然为null!");
			}
		}
		return value;
	}
	
	/**
	 * 读取配置信息（不支持重新加载配置文件）
	 * 
	 * @param name
	 * @return
	 */
	public static String getProperty(String name) {
		return properties.getProperty(name);
	}

	/**
	 * @param name
	 *            要找的int类型属性名
	 * @param number
	 *            找不到相关属性时默认返回的int类型数据
	 * */
	public static int getIntProperty(String name, int number) {
		String str = properties.getProperty(name);
		if (str == null) {
			return number;
		}
		return Integer.parseInt(str);
	}

	/**
	 * @param name
	 *            要找的long类型属性名
	 * @param number
	 *            找不到相关属性时默认返回的long类型数据
	 * */
	public static long getLongProperty(String name, long number) {
		String str = properties.getProperty(name);
		if (str == null) {
			return number;
		}
		return Long.parseLong(str);
	}

	/**
	 * @param name
	 *            要找的boolean类型属性名
	 * @param bool
	 *            找不到相关属性时默认返回的boolean数据
	 * */
	public static boolean getBooleanProperty(String name, boolean bool) {
		String str = properties.getProperty(name);
		if (str == null) {
			return bool;
		}
		return Boolean.parseBoolean(str);
	}
}
