
package com.ucar.streamsuite.common.hbase.util;

import com.ucar.streamsuite.common.config.ConfigProperty;
import com.ucar.streamsuite.common.util.DateUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

/**
 *  hbase utils
 * <br/> Created on 2014-9-2 下午1:13:14
 *
 * @since 3.4
 */
public final class HBaseUtils {


	public static final String DEFAULT_FAMILIY_NAME  = "commonFamily";

	/**
	 * 获取最小行健增量，用于类分页查询使用
	 *
	 * <br/> Created on 2014-9-25 上午9:20:16
	 *
	 * @since 3.4
	 * @return
	 */
	public static byte[] getMinIncrease(){
		return HBaseRowKeyUtils.getMinIncrease();
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
		return HBaseRowKeyUtils.getThisTimeAsc(timeMill);
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
		return HBaseRowKeyUtils.getThisTimeDesc();
	}

	public static long getThisTimeDesc(long time){
		return HBaseRowKeyUtils.getThisTimeDesc(time);
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
		return HBaseRowKeyUtils.getDescEnd(thisCurrentTimeMillis);
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
		return HBaseRowKeyUtils.getDescStart(thisCurrentTimeMillis);
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
		return HBaseRowKeyUtils.getFixedLength(id);
	}

	/**
	 * 获得一个字符串的字节数组
	 * @param value
	 * @return
	 */
	public static byte[] getBytes(String value) {
		if (value == null) {
			return null;
		}
		return Bytes.toBytes(value);
	}

	/**
	 * 得到降序时间字符串
	 * @param strDate
	 * @return
	 * @throws ParseException
	 */
	public static String getReversalTimeStr(String strDate) {
		String result = "";
		if (StringUtils.isBlank(strDate)) {
			return result;
		}
		Calendar calendar = DateUtil.getCalendar(strDate);
		if(calendar == null){
			return result;
		}
		result = getThisTimeDesc(calendar.getTime().getTime()) + "";
		return result;
	}
}
