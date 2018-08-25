package code.vikashs.hbase.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseFilter {
	public static void printResult(ResultScanner scanResult) {
		for (Result res : scanResult) {
			for (Cell cell : res.listCells()) {
				String row = new String(CellUtil.cloneRow(cell));
				String column = new String(CellUtil.cloneQualifier(cell));
				String value = new String(CellUtil.cloneValue(cell));
				System.out.println("value is"+ row +" "+ column +"  "+ value);
			}
		}
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		//conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
		//conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table=null;
		ResultScanner scanResult=null;
		try {
			table=connection.getTable(TableName.valueOf("census"));
			Filter filter =new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("4")));
			Scan scan=new Scan();
			scan.setFilter(filter);
			scanResult=table.getScanner(scan);
			printResult(scanResult);
			
		} finally {
			// TODO: handle finally clause
			connection.close();
			if(table !=null){
				table.close();
			}
			if(scanResult !=null){
				scanResult.close();
			}
		}

	}

}
