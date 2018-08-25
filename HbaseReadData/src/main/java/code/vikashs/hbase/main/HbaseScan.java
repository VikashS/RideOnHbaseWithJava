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

public class HbaseScan {

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
			Scan scan=new Scan();
			scanResult=table.getScanner(scan);
			for(Result res: scanResult){
				for(Cell cell: res.listCells()){
					String row=new String(CellUtil.cloneRow(cell));
					String column=new String(CellUtil.cloneQualifier(cell));
					String value=new String(CellUtil.cloneValue(cell));
				}
				
			}
			
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
