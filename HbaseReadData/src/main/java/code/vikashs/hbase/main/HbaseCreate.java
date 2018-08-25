package code.vikashs.hbase.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HbaseCreate {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		//conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
		//conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		Connection connection = ConnectionFactory.createConnection(conf);

		try {
			Admin admin = connection.getAdmin();
			HTableDescriptor tableName = new HTableDescriptor(TableName.valueOf("census"));
			tableName.addFamily(new HColumnDescriptor("personal"));
			tableName.addFamily(new HColumnDescriptor("professional"));

			if (!admin.tableExists(tableName.getTableName())) {
				admin.createTable(tableName);
				System.out.println("Table created");
			} else {
				System.out.println("table already exist");
			}

		} finally {
			// TODO: handle finally clause
			connection.isClosed();
		}

	}

}
