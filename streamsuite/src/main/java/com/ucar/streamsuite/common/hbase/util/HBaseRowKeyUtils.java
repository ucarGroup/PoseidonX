package com.ucar.streamsuite.common.hbase.util;

import java.util.Random;

/**
 * Hbase-Rowkey-工具类
 *
 *
 */
public class HBaseRowKeyUtils {
	
	private static final long DEFAULT_FUTURE = 99999999999999999l;

	private static final long DEFAULT_FIXED = 1000000000;

	private static final int RANDOM_LENGTH = 10;
	
	private static final int LENGTH = 4;

	/**
	 * 获取最小行健增量，用于类分页查询使用
	 *
	 * <br/> Created on 2014-9-25 上午9:20:16
	 *
	 * @since 3.4
	 * @return
	 */
	public static byte[] getMinIncrease(){
		byte[] bytes = new byte[1];
		bytes[0] = 0 ;
		return bytes ;
	}

	/**
	 * 升序获取当前毫秒数+4位随机数
	 *
	 * <br/> Created on 2014-9-23 下午1:00:14
	 *
	 * @since 3.4
	 * @param timeMill
	 * @return
	 */
	public static String getThisTimeAsc(long timeMill){
		long time = System.currentTimeMillis();
		if(timeMill >0){
			time = timeMill;
		}

		StringBuilder sb = new StringBuilder(String.valueOf(time));
		String random = randoms();
		sb.append(random);
		return sb.toString();
	}

	/**
	 * 获取当前时间逆序
	 *
	 * <br/> Created on 2014-9-2 下午2:20:12
	 *
	 * @since 3.4
	 * @return
	 */
	public static long getThisTimeDesc(){
		return getThisTimeDesc(System.currentTimeMillis());
	}

	public static long getThisTimeDesc(long time){
		StringBuilder sb = new StringBuilder(String.valueOf(time));
		while(true){
			String random = randoms();
			if(!random.equals("0000")){
				sb.append(random);
				break ;
			}
		}

		return DEFAULT_FUTURE - Long.parseLong(sb.toString());
	}

	/**
	 * 获取结束 rowkey
	 *
	 * <br/> Created on 2014-9-17 下午3:46:51
	 *
	 * @since 3.4
	 * @param thisCurrentTimeMillis
	 * @return
	 */
	public static long getDescEnd(long thisCurrentTimeMillis){
		StringBuilder sb = new StringBuilder(String.valueOf(thisCurrentTimeMillis));
		sb.append("0000");
		return DEFAULT_FUTURE - Long.parseLong(sb.toString());
	}

	/**
	 * 获取开始rowkey
	 *
	 *
	 * <br/> Created on 2014-9-17 下午3:47:00
	 *
	 * @since 3.4
	 * @param thisCurrentTimeMillis,为0时，默认当前时间
	 * @return
	 */
	public static long getDescStart(long thisCurrentTimeMillis){
		long millis = System.currentTimeMillis();
		if(thisCurrentTimeMillis > 0){
			millis = thisCurrentTimeMillis;
		}
		StringBuilder sb = new StringBuilder(String.valueOf(millis));
		sb.append("9999");
		return DEFAULT_FUTURE - Long.parseLong(sb.toString());
	}

	/**
	 * 获取固定长度的 db 主键值
	 *
	 * <br/> Created on 2014-9-9 上午11:48:35
	 *
	 * @since 3.4
	 * @param id
	 * @return
	 */
	public static long getFixedLength(long id){
		return DEFAULT_FIXED + id ;
	}

	private static String randoms(){
		Random random = new Random();
		StringBuilder sb = new StringBuilder();
		for(int i =0;i<LENGTH;i++){
			sb.append(random.nextInt(RANDOM_LENGTH));
		}
		return sb.toString();
	}
}
