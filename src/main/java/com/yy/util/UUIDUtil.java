package com.yy.util;

import java.util.UUID;

/**
 * UUID工具类
 * 
 * @author 老彭
 * 
 */
public class UUIDUtil {

	/**
	 * 随机生成UUID字符串
	 * 
	 * @return
	 */
	public static String generateUUIDString() {
		return compressedUUID(UUID.randomUUID());
	}

	private static String compressedUUID(UUID uuid) {
		byte[] byUuid = new byte[16];
		long least = uuid.getLeastSignificantBits();
		long most = uuid.getMostSignificantBits();
		long2bytes(most, byUuid, 0);
		long2bytes(least, byUuid, 8);
		String compressUUID = Base58.encode(byUuid);
		return compressUUID;
	}

	private static void long2bytes(long value, byte[] bytes, int offset) {
		for (int i = 7; i > -1; i--) {
			bytes[offset++] = (byte) ((value >> 8 * i) & 0xFF);
		}
	}
}
