
package com.ucar.streamsuite.common.hbase.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.ucar.streamsuite.common.hbase.CommonHBaseTable;
import com.ucar.streamsuite.common.hbase.CommonHbaseRecord;
import com.ucar.streamsuite.common.hbase.vo.*;
import com.ucar.streamsuite.common.hbase.HTable;

import com.ucar.streamsuite.common.util.DateUtil;
import com.ucar.streamsuite.moniter.constants.MoniterContant;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HBaseRecordUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseRecordUtils.class);

	private final static ThreadPoolExecutor saveHBaseRecordService = new ThreadPoolExecutor(2, 10, 1,
			TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(10000), new ThreadPoolExecutor.CallerRunsPolicy());

	private static class SaveHBaseRecordThread implements Runnable {
		private CommonHbaseRecord commonHbaseRecord;
		public SaveHBaseRecordThread(CommonHbaseRecord commonHbaseRecord) {
			this.commonHbaseRecord = commonHbaseRecord;
		}
		@Override
		public void run() {
			send(commonHbaseRecord);
		}
	}

	public static void asynSend(CommonHbaseRecord commonHbaseRecord) {
		if (commonHbaseRecord == null
				|| StringUtils.isBlank(commonHbaseRecord.getTableName())
				|| StringUtils.isBlank(commonHbaseRecord.getFamilyName())
				|| StringUtils.isBlank(commonHbaseRecord.getRowKey())
				|| CollectionUtils.isEmpty(commonHbaseRecord.getHbaseCellList()) ){
            //日志
			return;
		}
		saveHBaseRecordService.submit(new SaveHBaseRecordThread(commonHbaseRecord));
	}

	/**
	 * 根据 scanVo 获得查询结果
	 * @param scanVo
	 * @return
	 */
	public static List<RowVo> getScanResult(ScanVo scanVo) {
		List<RowVo> results = new ArrayList<RowVo>();
		//校验
        if(scanVo == null || StringUtils.isBlank(scanVo.getTableName())|| scanVo.getDate()==null || StringUtils.isBlank(scanVo.getStartKey())|| StringUtils.isBlank(scanVo.getEndKey())){
        	return results;
		}
		try {
			Integer i = 0;
			String tableFullName = scanVo.getFamilyName() + scanVo.getTableName();
			CommonHBaseTable commonHbaseTable = CommonHbaseTableImpl.tableCache.get(tableFullName);
			if (commonHbaseTable == null) {
				commonHbaseTable = new CommonHbaseTableImpl(scanVo.getFamilyName(), scanVo.getTableName());
				CommonHbaseTableImpl.tableCache.putIfAbsent(tableFullName,commonHbaseTable);
			}
			HTable htable = commonHbaseTable.getTable(scanVo.getDate(),true);
			scanVo.setTableName(htable.getTableName());
			ScanResult scanResult =  htable.getScanner(scanVo);
			Iterator<RowVo> rowIterator = scanResult.iterator();
			while(rowIterator.hasNext() && i<scanVo.getLimit()){
				RowVo row = rowIterator.next();
				results.add(row);
				i ++;
			}
		} catch (Exception e) {
			LOGGER.error("getScanResult is error", e);
		}
		return results;
	}

	/**
	 * 获得hbase的查询对象
	 * @param time
	 * @return
	 */
	public static ScanVo getScanVO(String tableName,String time,String startRowKey,String endRowKey,Integer limit) {
		try {
			ScanVo scanVo = new ScanVo();
			Calendar calendar = DateUtil.getCalendar(time);
			if(calendar == null){
				return null;
			}
			scanVo.setDate(calendar.getTime());
			scanVo.setStartKey(startRowKey);
			scanVo.setEndKey(endRowKey);
			scanVo.setLimit(limit);
			scanVo.setTableName(tableName);
			return scanVo;
		} catch (Exception e) {
			LOGGER.error("getScanVO is error",e);
			return null;
		}
	}

	public static void send(CommonHbaseRecord commonHbaseRecord) {
		send(commonHbaseRecord,true);
	}

	/**
	 *
	 * @param commonHbaseRecord
	 * @param isSplit //表示是否按照月分表, true为分表,false为不分表
	 */
	public static void send(CommonHbaseRecord commonHbaseRecord,boolean isSplit) {
		try {
			if (commonHbaseRecord == null
					|| StringUtils.isBlank(commonHbaseRecord.getTableName())
					|| StringUtils.isBlank(commonHbaseRecord.getFamilyName())
					|| StringUtils.isBlank(commonHbaseRecord.getRowKey())
					|| CollectionUtils.isEmpty(commonHbaseRecord.getHbaseCellList()) ){
				return;
			}
			String tableFullName = commonHbaseRecord.getFamilyName() + commonHbaseRecord.getTableName();
			CommonHBaseTable commonHbaseTable = CommonHbaseTableImpl.tableCache.get(tableFullName);
			if (commonHbaseTable == null) {
				commonHbaseTable = new CommonHbaseTableImpl(commonHbaseRecord.getFamilyName(), commonHbaseRecord.getTableName());
				((CommonHbaseTableImpl)commonHbaseTable).setSplit(isSplit);
				CommonHbaseTableImpl.tableCache.putIfAbsent(tableFullName,commonHbaseTable);
			}

			HTable htable = commonHbaseTable.getHTable();

			PutVo put = new PutVo();
			put.setRowKey(Bytes.toBytes(commonHbaseRecord.getRowKey()));

			List<CommonHbaseRecord.HbaseCellItem> hbaseCellItemList = commonHbaseRecord.getHbaseCellList();
			List<Column> putList = new ArrayList<Column>();
			for (CommonHbaseRecord.HbaseCellItem hbaseCellItem : hbaseCellItemList) {
				Column column = new Column();
				column.setFamily(Bytes.toBytes(hbaseCellItem.getFamilyName()));
				column.setQualifier(Bytes.toBytes(hbaseCellItem.getColumnName()));
				column.setValue(hbaseCellItem.getValue());
				putList.add(column);
			}
			put.setColumns(putList);
		    htable.put(put);
		} catch (Exception e) {
			LOGGER.error("HBaseRecordUtils 插入数据错误", e);
		}
	}

	private static class CommonHbaseTableImpl extends CommonHBaseTable {
		/**
		 * 缓存表名:table 实例
		 */
		private final static ConcurrentHashMap<String,CommonHBaseTable> tableCache = new ConcurrentHashMap<String, CommonHBaseTable>();

		private String familyName;
		private boolean isSplit = true ;

		public CommonHbaseTableImpl(String familyName,String tableName) {
			super(tableName);
			this.familyName = familyName;
		}

		@Override
		public void addFamily(TableDescriptionVo tableDescriptionVo) {
			tableDescriptionVo.getListColumn().add(new ColumnDescriptor(familyName, true));
		}

		public void setSplit(boolean split) {
			isSplit = split;
		}

		/**
		 * 默认按照月分表，返回空不做分表处理
		 */
		protected SimpleDateFormat getSplitTableFormat(){
			if(isSplit) {
				return new SimpleDateFormat("yyyyMM");
			}
			else{
				return null;
			}
		}
	}


}
