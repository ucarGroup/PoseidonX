package com.ucar.streamsuite.common.hbase;

import com.ucar.streamsuite.common.hbase.vo.*;
import com.ucar.streamsuite.common.hbase.vo.Column;
import com.ucar.streamsuite.common.hbase.vo.DeleteVo;
import com.ucar.streamsuite.common.hbase.vo.GetVo;
import com.ucar.streamsuite.common.hbase.vo.HbaseRemoteVo;
import com.ucar.streamsuite.common.hbase.vo.IncrementVo;
import com.ucar.streamsuite.common.hbase.vo.PutVo;
import com.ucar.streamsuite.common.hbase.vo.RowVo;
import com.ucar.streamsuite.common.hbase.vo.ScanVo;
import com.ucar.streamsuite.common.hbase.vo.TableDescriptionVo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import java.io.IOException;
import java.util.*;

/**
 * Created on 2016/6/27.
 * Description : HBase操作API
 */
public class HTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HTable.class);

    private static final ThreadLocal<Map<String,List<PutVo>>> CURRENT_THREAD_PUTS = new ThreadLocal<Map<String, List<PutVo>>>();

    private static HTableOperateInterface hbaseClient = new HTableOperatorImpl();

    // 当前线程是否自动提交。如果不设置或为true 则为直接提交。如果为false 则 put时 CURRENT_THREAD_PUTS 大于bufferSize时才提交
    // 默认情况下是直接提交
    private ThreadLocal<Boolean> autoFlushLocal = new ThreadLocal<Boolean>();

    private TableDescriptionVo tableDescription;

    private volatile int bufferSize = 10;

    private String splitTableValue;

    /**
     * 构造函数用于获得 htable
     * @param tableName
     */
    public HTable(String tableName) {
        this.tableDescription = new TableDescriptionVo();
        this.tableDescription.setTableName(tableName);
    }

    /**
     * 构造函数用于建表
     * @param tableName
     */
    public HTable(TableDescriptionVo tableDescription, String splitTableValue) {
        this.tableDescription = tableDescription;
        this.splitTableValue = splitTableValue;
        try {
            hbaseClient.createTable(tableDescription);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ScanResult getScanner(ScanVo vo) {
        return new ResultScannerProxy(this, vo);
    }

    public void put(PutVo putVo) throws IOException {
        validatePut(putVo);
        Boolean autoFlush = autoFlushLocal.get();
        if (autoFlush == null || autoFlush) {
            List<PutVo> putVos = new ArrayList<PutVo>();
            putVos.add(putVo);
            directCommit(putVos);
            return;
        }
        doPut(putVo);
    }

    private void doPut(PutVo putVo) throws IOException {
        Map<String,List<PutVo>> currentPutsMap = CURRENT_THREAD_PUTS.get();
        if (currentPutsMap == null) {
            currentPutsMap = new HashMap<String, List<PutVo>>();
            CURRENT_THREAD_PUTS.set(currentPutsMap);
        }
        List<PutVo> puts = currentPutsMap.get(this.getTableName());
        if (puts == null) {
            puts = new ArrayList<PutVo>();
            currentPutsMap.put(this.getTableName(), puts);
        }
        puts.add(putVo);
        if (puts.size() > bufferSize) {
            flushCommits();
        }
    }

    /**
     * 验证put数据的合法性
     * @param putVo
     */
    private void validatePut(PutVo putVo) {
        if (putVo == null) {
            throw new IllegalArgumentException("put couldn't be null");
        }

        List<Column> columns = putVo.getColumns();
        if (columns != null && columns.size() > 0) {
            for (Column column : columns) {
                byte[] family = column.getFamily();
                if (family == null) {
                    throw new IllegalArgumentException("family couldn't be null for rowKey " + putVo.getRowKey());
                }
            }
        } else {
            throw new IllegalArgumentException("columns couldn't be null for rowKey " + putVo.getRowKey());
        }
    }

    /**
     * 直接提交
     * @param putVos
     * @throws IOException
     */
    public void  directCommit(List<PutVo> putVos) throws IOException {
        long start = System.currentTimeMillis();
        try {
            HbaseRemoteVo bufferVo = new HbaseRemoteVo();
            bufferVo.setTableName(tableDescription.getTableName());
            bufferVo.setTableDescriptionVo(tableDescription);
            bufferVo.setPutVos(putVos);
            hbaseClient.put(bufferVo);
        }finally {
            long consume = System.currentTimeMillis() - start;
            LOGGER.info(" commit puts (size : " + putVos.size()  + ") consume : " + consume + " ms.");
            putVos.clear();
        }
    }

    public void flushCommits() throws IOException {
        Map<String,List<PutVo>> currentPutsMap = CURRENT_THREAD_PUTS.get();
        if (currentPutsMap != null) {
            List<PutVo> list = currentPutsMap.get(this.getTableName());
            if (list != null) {
                directCommit(list);
            }
        }
    }

    public void setAutoFlush(boolean autoFlush) {
        autoFlushLocal.set(autoFlush);
    }

    public List<RowVo> scan(ScanVo vo) {
        long start = System.currentTimeMillis();
        try {
            if (StringUtils.isEmpty(vo.getTableName())) {
                vo.setTableName(this.getTableName());
            }
            List<RowVo>  listRow=hbaseClient.scan(vo);
            return listRow;
        }finally {
            long consume = System.currentTimeMillis() - start;
            LOGGER.info("scan table " + vo.getTableName() + "  consume " + consume + " ms.");
        }
    }

    public RowVo get(GetVo getVo) {
        long start = System.currentTimeMillis();
        try {
            if (StringUtils.isEmpty(getVo.getTableName())) {
                getVo.setTableName(this.getTableName());
            }
            RowVo rowVo = hbaseClient.get(getVo);
            return rowVo;
        }finally {
            long consume = System.currentTimeMillis() - start;
            LOGGER.info("get table " + getVo.getTableName() + "  consume " + consume + " ms.");
        }
    }

    public List<RowVo> get(List<GetVo> getVos) {
        boolean succ = false;
        long start = System.currentTimeMillis();
        try {
            for (GetVo get : getVos) {
                if (StringUtils.isEmpty(get.getTableName())) {
                    get.setTableName(this.getTableName());
                }
            }
            List<RowVo> rowVos = hbaseClient.get(getVos);
            succ = true;
            return rowVos;
        }finally {
            long consume = System.currentTimeMillis() - start;
            LOGGER.info("get table list " + this.getTableName() + "  consume " + consume + " ms.");
        }

    }

    public long incrementColumnValue(IncrementVo incrementVo) {
        long start = System.currentTimeMillis();
        try {
            if (StringUtils.isEmpty(incrementVo.getTableName())) {
                incrementVo.setTableName(this.getTableName());
            }
            return hbaseClient.increase(incrementVo);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            long consume = System.currentTimeMillis() - start;
            LOGGER.info("increase " + this.getTableName() + "  consume " + consume + " ms.");
        }
    }

    public void delete(DeleteVo deleteVo) {
        boolean succ = false;
        long start = System.currentTimeMillis();
        try {
            if (StringUtils.isEmpty(deleteVo.getTableName())) {
                deleteVo.setTableName(this.getTableName());
            }
            hbaseClient.delete(deleteVo);
            succ = true;
        }finally {
            long consume = System.currentTimeMillis() - start;
            LOGGER.info("delete table " + this.getTableName() + " consume " + consume + " ms.");
        }
    }

    public void delete(List<DeleteVo> deleteVos) {
        long start = System.currentTimeMillis();
        try {
            for (DeleteVo deleteVo : deleteVos) {
                if (StringUtils.isEmpty(deleteVo.getTableName())) {
                    deleteVo.setTableName(this.getTableName());
                }
            }
            hbaseClient.delete(deleteVos);
        }finally {
            long consume = System.currentTimeMillis() - start;
            LOGGER.info("delete table " + this.getTableName() + " consume " + consume + " ms.");
        }
    }

    public void close () {
        try {
            flushCommits();
        } catch (IOException e) {
            LOGGER.error("flush error when close table.",e);
        }
    }

    public String getTableName() {
        return this.tableDescription.getTableName();
    }

    public TableDescriptionVo getTableDescription() {
        return tableDescription;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    /**
     * Description : 结果迭代器
     */
    private class ResultScannerProxy implements ScanResult {

        private final HTable hTable;

        private ScanVo scanVo;

        private Iterator<RowVo> cachedIterator;

        public ResultScannerProxy(HTable hTable, ScanVo scanVo) {
            this.hTable = hTable;
            this.scanVo = scanVo;
        }

        @Override
        public RowVo next() throws IOException {
            if (cachedIterator != null && cachedIterator.hasNext()) {
                return cachedIterator.next();
            }
            List<RowVo> rowVoList = hTable.scan(scanVo);
            if (rowVoList != null && rowVoList.size() > 0) {
                // 更新行健
                RowVo rowVo = rowVoList.get(rowVoList.size() - 1);
                scanVo.setStartKey(rowVo.getRowKey() + "~");

                cachedIterator = rowVoList.iterator();
                return cachedIterator.next();
            }
            return null;
        }

        @Override
        public RowVo[] next(int nbRows) throws IOException {
            int num = 0;
            List<RowVo> rowVoList = new LinkedList<RowVo>();
            while (num < nbRows) {
                RowVo rowVo = next();
                if (rowVo != null) {
                    rowVoList.add(rowVo);
                } else {
                    break;
                }
                ++num;
            }
            if (rowVoList.size() > 0) {
                return rowVoList.toArray(new RowVo[rowVoList.size()]);
            } else {
                return null;
            }
        }

        @Override
        public Iterator<RowVo> iterator() {
            return new Iterator<RowVo>() {
                RowVo next = null;
                public boolean hasNext() {
                    if (next == null) {
                        try {
                            next = ResultScannerProxy.this.next();
                            return next != null;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return true;
                }
                public RowVo next() {
                    if (!hasNext()) {
                        return null;
                    }
                    RowVo temp = next;
                    next = null;
                    return temp;
                }
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}
