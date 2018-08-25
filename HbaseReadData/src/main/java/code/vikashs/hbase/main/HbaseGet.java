package code.vikashs.hbase.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseGet {
	private static byte[] PERSONAL_CF = Bytes.toBytes("personal");
	private static byte[] PROFESSIONAL_CF = Bytes.toBytes("professional");
	private static byte[] NAME_COLUMN = Bytes.toBytes("name");
	private static byte[] FIELD_COLUMN = Bytes.toBytes("field");

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		//conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
		//conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table=null;
		
		try {
			table=connection.getTable(TableName.valueOf("census"));
			Get get =new Get(Bytes.toBytes("1"));
			get.addColumn(PERSONAL_CF, NAME_COLUMN);
			get.addColumn(PROFESSIONAL_CF, FIELD_COLUMN);
			Result result=table.get(get);
			
			byte[] nameValue=result.getValue(PERSONAL_CF, NAME_COLUMN);
			byte[] fieldValue=result.getValue(PROFESSIONAL_CF, FIELD_COLUMN);
			System.out.println("value is "+ Bytes.toString(nameValue));
			System.out.println("field is "+ Bytes.toString(fieldValue));
			
		} finally {
			// TODO: handle finally clause
			connection.close();
			if(table !=null){
				table.close();
			}
		}

	}

}
