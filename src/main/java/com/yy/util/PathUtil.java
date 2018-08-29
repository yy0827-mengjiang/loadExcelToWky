package com.yy.util;

import java.io.File;

public class PathUtil {

	/**
	 * 获取配件文件路径，配置文件默认在运行jar当前路径的conf/文件夹内
	 * 
	 * @return
	 */
	public static String getConfigPath(String fileName) {
		String jarPath = getJarDir();
		if (jarPath != null) {
			return jarPath + "//config//" + fileName;
		}
		return null;
	}

	/**
	 * 获取jar绝对路径
	 * 
	 * @return
	 */
	public static String getJarPath() {
		File file = getFile();
		if (file == null)
			return null;
		return file.getAbsolutePath();
	}

	/**
	 * 获取jar目录
	 * 
	 * @return
	 */
	public static String getJarDir() {
		File file = getFile();
		if (file == null)
			return null;
		return getFile().getParent();
	}

	/**
	 * 获取jar包名
	 * 
	 * @return
	 */
	public static String getJarName() {
		File file = getFile();
		if (file == null)
			return null;
		return getFile().getName();
	}

	/**
	 * 获取当前Jar文件
	 * 
	 * @return
	 */
	private static File getFile() {
		String path = PathUtil.class.getProtectionDomain().getCodeSource()
				.getLocation().getFile();
		try {
			path = java.net.URLDecoder.decode(path, "UTF-8"); // 转换处理中文及空格
		} catch (java.io.UnsupportedEncodingException e) {
			return null;
		}
		return new File(path);
	}
}
