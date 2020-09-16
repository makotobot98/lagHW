package com.makoto.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class MyCoprocessor extends BaseRegionObserver{
    private static final String TABLE_NAME = "user_relation";
    private static final String FAMAILLY_NAME = "friends";
    
    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        HTable tb = (HTable) e.getEnvironment().getTable(TableName.valueOf(TABLE_NAME));
        //delete: https://hbase.apache.org/1.2/apidocs/org/apache/hadoop/hbase/client/Delete.html
        //usage: https://www.pianshen.com/article/38701291897/
        List<Cell> cells = delete.getFamilyCellMap().get(Bytes.toBytes(FAMAILLY_NAME));
        for (Cell cell : cells) {
            //obtain uid bytes
            byte[] uid = CellUtil.cloneQualifier(cell);

            //obtain current row key
            byte[] myuid = CellUtil.cloneRow(cell);

            //add deletion column and its column family
            Delete d = new Delete(uid);
            d.addColumn(Bytes.toBytes(FAMAILLY_NAME), myuid);
            tb.delete(d);
        }

        tb.close();
    }
}
