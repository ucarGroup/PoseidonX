package com.ucar.streamsuite.common.hbase;

import com.ucar.streamsuite.common.hbase.vo.*;
import com.ucar.streamsuite.common.hbase.proxy.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created on 2017/3/24.
 * Description : HBase 原生 api 操作
 */
public class HTableOperatorImpl implements HTableOperateInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(HTableOperatorImpl.class);

    @Override
    public void createTable(TableDescriptionVo tableDescVo)  {
        synchronized (this) {
            HBaseAdmin hBaseAdmin = null;
            try{
                Configuration configuration = HBaseManager.getConfiguration();
                hBaseAdmin = new HBaseAdmin(configuration);
                if (hBaseAdmin.tableExists(tableDescVo.getTableName())){
                    return;
                }
                LOGGER.error("table " + tableDescVo.getTableName() + " is not exsit ,begin to create");
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableDescVo.getTableName());
                //512M region max size
                tableDescriptor.setMaxFileSize(tableDescVo.getFileSize());
                //添加列族
                addFamily(tableDescVo.getListColumn(), tableDescriptor);

                //设置 memstore flush size
                tableDescriptor.setMemStoreFlushSize(tableDescVo.getMemStoreSize());

                if (tableDescVo.getSplitKeys() != null) {
                    hBaseAdmin.createTable(tableDescriptor, tableDescVo.getSplitKeys());
                } else {
                    hBaseAdmin.createTable(tableDescriptor);
                }
                LOGGER.error("table " + tableDescVo.getTableName() + " is created successfully !");
            } catch (Throwable e) {
                LOGGER.error("建表失败！" + e.getMessage(), e);
            } finally {
                if(hBaseAdmin != null) {
                    try {
                        hBaseAdmin.close();
                    } catch (Exception e) {
                    }
                }
            }
        }
    }

    @Override
    public void put(HbaseRemoteVo vo) {
        String tableName = vo.getTableName();
        ProxyTable table = null ;
        try{
            table = HProxyTablePools.getHtable(tableName, new CommandsParameterVo());
            table.setAutoFlushTo(false);
            for (PutVo putVo : vo.getPutVos()) {
                Put put = new Put(putVo.getRowKey());
                for(Column column: putVo.getColumns()) {
                    put.add(column.getFamily(), column.getQualifier(), column.getValue());
                }
                put.setWriteToWAL(putVo.isWriteToWAL());
                table.put(put);
            }
            table.flushCommits();
        } catch (InterruptedIOException e) {
        } catch (Exception e) {
            if(table != null){
                table.setFail(true);
            }
            throw new RuntimeException(e);
        }finally{
            if(table != null){
                HProxyTablePools.returnTable(tableName, table);
            }
        }
    }

    @Override
    public RowVo get(GetVo getVo) {
        ProxyTable table = null;
        try {
            table = HProxyTablePools.getHtable(getVo.getTableName(), new CommandsParameterVo());
            Get get = new Get(getVo.getByteRowKey());
            Result result = table.get(get);
            List<Column> columns = new ArrayList<Column>();
            for (Cell cell : result.rawCells()) {
                Column column = new Column();
                column.setFamily(CellUtil.cloneFamily(cell));
                column.setQualifier(CellUtil.cloneQualifier(cell));
                column.setValue(CellUtil.cloneValue(cell));
                columns.add(column);
            }
            RowVo rowVo = new RowVo();
            rowVo.setRowKey(Bytes.toString(getVo.getByteRowKey()));
            rowVo.setColumns(columns);
            return rowVo;
        } catch (Exception e) {
            if(table != null){
                table.setFail(true);
            }
            LOGGER.error("get table " + getVo.getTableName() + " row :" + getVo.getRowKey() + " failed.", e);
            throw new RuntimeException(e);
        } finally {
            if (table != null) {
                HProxyTablePools.returnTable(getVo.getTableName(), table);
            }
        }
    }

    @Override
    public List<RowVo> get(List<GetVo> getVoList) {
        ProxyTable table = null;
        String tableName = null;
        try {
            if (getVoList != null && getVoList.size() > 0) {
                tableName = getVoList.get(0).getTableName();
            }else{
                throw new NullPointerException();
            }
            table = HProxyTablePools.getHtable(tableName, new CommandsParameterVo());
            List<Get> getList = new ArrayList<Get>();
            for (GetVo vo : getVoList) {
                Get get = new Get(vo.getByteRowKey());
                getList.add(get);
            }
            Result[] results = table.get(getList);
            if (results != null) {
                List<RowVo> rowVoList = new ArrayList<RowVo>();
                for (Result result : results) {
                    List<Column> columns = new ArrayList<Column>();
                    for (Cell cell : result.rawCells()) {
                        Column column = new Column();
                        column.setFamily(CellUtil.cloneFamily(cell));
                        column.setQualifier(CellUtil.cloneQualifier(cell));
                        column.setValue(CellUtil.cloneValue(cell));
                        columns.add(column);
                    }
                    RowVo rowVo = new RowVo();
                    rowVo.setRowKey(Bytes.toString(result.getRow()));
                    rowVo.setColumns(columns);
                    rowVoList.add(rowVo);
                }
                return rowVoList;
            } else {
                return null;
            }
        } catch (Exception e) {
            StringBuilder rowsBuilder = new StringBuilder().append("\n");
            for (GetVo getVo : getVoList) {
                rowsBuilder.append(getVo.getRowKey()).append("\n");
            }
            if(table != null){
                table.setFail(true);
            }
            LOGGER.error("get table " + tableName + " rows :" + rowsBuilder.toString() + " failed.", e);
            throw new RuntimeException(e);
        } finally {
            if (table != null) {
                HProxyTablePools.returnTable(tableName, table);
            }
        }
    }

    @Override
    public List<RowVo> scan(ScanVo vo) {
        ProxyTable table = null ;
        List<RowVo> resultList = new ArrayList<RowVo>(vo.getCacheCount());
        try{
            CommandsParameterVo cpv = vo.getCpvo() == null?new CommandsParameterVo():vo.getCpvo();
            table = HProxyTablePools.getHtable(vo.getTableName(), cpv);
            Scan scan = new Scan();
            scan.setCaching(vo.getCacheCount());
            scan.setReversed(vo.isReversed());
            if(!StringUtils.isEmpty(vo.getStartKey())){
                scan.setStartRow(Bytes.toBytes(vo.getStartKey()));
            }
            if(!StringUtils.isEmpty(vo.getEndKey())){
                scan.setStopRow(Bytes.toBytes(vo.getEndKey()));
            }

            if (vo.getScanFamilyMap() != null && vo.getScanFamilyMap().size() > 0) {
                for (Map.Entry<String, List<String>> entry : vo.getScanFamilyMap().entrySet()) {
                    byte[] family = Bytes.toBytes(entry.getKey());
                    scan.addFamily(family);
                    if (entry.getValue() != null) {
                        for (String qualifier : entry.getValue()) {
                            scan.addColumn(family, Bytes.toBytes(qualifier));
                        }
                    }
                }
            }

            if(vo.getScanComparisons() != null && vo.getScanComparisons().size() > 0) {
                scan.setFilter(getFilterList(vo.getScanComparisons(), vo.getOperator()));
            }

            ResultScanner scanner =  table.getScanner(scan);
            Iterator<Result> ite = scanner.iterator();
            int count = 0 ;
            while(ite.hasNext()){
                Result result = ite.next();
                RowVo rvo = new RowVo();
                List<Column> columns = new ArrayList<Column>();
                rvo.setRowKey(Bytes.toString(result.getRow()));
                rvo.setColumns(columns);
                for (Cell cell : result.rawCells()) {
                    Column column = new Column();
                    column.setFamily(CellUtil.cloneFamily(cell));
                    column.setQualifier(CellUtil.cloneQualifier(cell));
                    column.setValue(CellUtil.cloneValue(cell));
                    columns.add(column);
                }
                resultList.add(rvo);
                count ++ ;
                if(count >= vo.getCacheCount()){
                    break ;
                }
            }

        }catch (Exception e) {
            if(table != null){
                table.setFail(true);
            }
            LOGGER.error("scan fail for user host : " + vo.getHostName() + " ip : " + vo.getLocalIp(), e);
        }finally{
            if(table != null){
                HProxyTablePools.returnTable(vo.getTableName(), table);
            }
        }
        return resultList;
    }

    @Override
    public boolean tableExists(String tableName) {
        HBaseAdmin hBaseAdmin = null ;
        try {
            Configuration configuration = HBaseManager.getConfiguration();
            hBaseAdmin = new HBaseAdmin(configuration);
            return hBaseAdmin.tableExists(tableName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally{
            if(hBaseAdmin != null){
                try {
                    hBaseAdmin.close();
                } catch (IOException e) {
                    LOGGER.error("", e);
                }
            }
        }
    }

    @Override
    public boolean tableExists(String clusterName, String tableName) {
        HBaseAdmin hBaseAdmin = null ;
        try {
            Configuration configuration = HBaseManager.getConfiguration();
            hBaseAdmin = new HBaseAdmin(configuration);
            return hBaseAdmin.tableExists(tableName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally{
            if(hBaseAdmin != null){
                try {
                    hBaseAdmin.close();
                } catch (IOException e) {
                    LOGGER.error("", e);
                }
            }
        }
    }

    @Override
    public void delete(DeleteVo deleteVo) {
        List<DeleteVo> deleteVos = new ArrayList<DeleteVo>();
        deleteVos.add(deleteVo);
        delete(deleteVos);
    }

    @Override
    public void delete(List<DeleteVo> deleteVos) {
        ProxyTable table = null;
        String tableName = null;
        try {
            if (deleteVos != null && deleteVos.size() > 0) {
                tableName = deleteVos.get(0).getTableName();
            }else{
                throw new NullPointerException();
            }
            table = HProxyTablePools.getHtable(tableName, new CommandsParameterVo());
            List<Delete> deletes = new ArrayList<Delete>();
            for (DeleteVo deleteVo : deleteVos) {
                Delete delete = new Delete(deleteVo.getRowKey());
                deletes.add(delete);
            }
            table.delete(deletes);
        } catch (Exception e) {
            StringBuilder rowsBuilder = new StringBuilder().append("\n");
            for (DeleteVo deleteVo : deleteVos) {
                rowsBuilder.append(Bytes.toString(deleteVo.getRowKey())).append("\n");
            }
            if(table != null){
                table.setFail(true);
            }

            LOGGER.error("delete table + " + tableName + " rows: " + rowsBuilder.toString() + " failed.");
            throw new RuntimeException(e);
        } finally {
            if (table != null) {
                HProxyTablePools.returnTable(tableName, table);
            }
        }
    }

    @Override
    public long increase(IncrementVo incrementVo) throws Exception {
        ProxyTable table = null;
        try {
            table = HProxyTablePools.getHtable(incrementVo.getTableName(),new CommandsParameterVo());
            return table.incrementColumnValue(incrementVo.getRowKey(), incrementVo.getFamily(), incrementVo.getQualifier(), incrementVo.getIncrementValue());
        } catch (Exception e) {
            throw e;
        } finally {
            if (table != null) {
                HProxyTablePools.returnTable(incrementVo.getTableName() , table);
            }
        }
    }

    private void addFamily(List<ColumnDescriptor> listColumn , HTableDescriptor tableDescriptor){
        for(ColumnDescriptor cd :  listColumn){
            tableDescriptor.addFamily(changeCd(cd));
        }
    }

    private HColumnDescriptor changeCd(ColumnDescriptor cd){
        HColumnDescriptor family = new HColumnDescriptor(cd.getFamilyName());
        if(cd.isCompress()){
            family.setCompactionCompressionType(Compression.Algorithm.GZ);
        }
        return family ;
    }

    private FilterList getFilterList(List<ScanComparisonVo> scanComparisons, FiltersOperatorVo.Operator operator) throws UnsupportedEncodingException {
        FilterList filterList;
        if (operator == null || operator.equals(FiltersOperatorVo.Operator.MUST_PASS_ALL)) {
            filterList  = new FilterList();
        } else {
            filterList  = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        }

        // 行健过滤器
        for(ScanComparisonVo vo : scanComparisons) {
            CompareFilter.CompareOp compareOp;
            switch (vo.getScanComparisonEnum()) {
                case LESS:
                    compareOp = CompareFilter.CompareOp.LESS;
                    break;
                case LESS_OR_EQUAL:
                    compareOp = CompareFilter.CompareOp.LESS_OR_EQUAL;
                    break;
                case EQUAL:
                    compareOp = CompareFilter.CompareOp.EQUAL;
                    break;
                case NOT_EQUAL:
                    compareOp = CompareFilter.CompareOp.NOT_EQUAL;
                    break;
                case GREATER_OR_EQUAL:
                    compareOp = CompareFilter.CompareOp.GREATER_OR_EQUAL;
                    break;
                case GREATER:
                    compareOp = CompareFilter.CompareOp.GREATER;
                    break;
                default:
                    compareOp = CompareFilter.CompareOp.EQUAL;
                    break;
            }

            if (vo.isRowFilter()) {
                filterList.addFilter(new RowFilter(compareOp, getComparator(vo.getValue(), vo.getScanComparatorEnum())));
            } else {
                filterList.addFilter(new SingleColumnValueFilter(vo.getFamily().getBytes("utf-8"),
                        vo.getQualifier().getBytes("utf-8"),
                        compareOp,
                        getComparator(vo.getValue(), vo.getScanComparatorEnum())));
            }
        }
        return filterList;
    }

    private ByteArrayComparable getComparator(Object value, ScanEnum.ScanComparatorEnum scanComparatorEnum) throws UnsupportedEncodingException {
        switch (scanComparatorEnum) {
            case EXACTSTRING:
                return new BinaryComparator(value.toString().getBytes("utf-8"));
            case SUBSTRING:
                return new SubstringComparator(value.toString());
            case REGEXSTRING:
                return new RegexStringComparator(value.toString());
            case LONG:
                return new LongComparator((Long)value);
            case BINARY:
                return new BinaryComparator((byte[])value);
            case BINARYPREFIX:
                return new BinaryPrefixComparator((byte[])value);
            case NULL:
                return new NullComparator();
            default:
                return new BinaryComparator(value.toString().getBytes("utf-8"));
        }
    }

}
