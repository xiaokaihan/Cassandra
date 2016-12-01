package com.key.cassandra.demo.trigger;

import java.io.InputStream;
import java.util.Properties;

import org.apache.cassandra.io.util.FileUtils;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

/**
 * 该类实现连接cassandra， 创建Keyspace和table。 
 * TriggerTest实现：	
 * 		创建了一个键空间，并在该键空间中分别创建两个不同的table, 然后在test表中配置一个trigger， 该trigger中关联的是表audit。 (AuditTrigger)。  
 * 		该trigger类需要已经被打包成jar并上传到cassandra服务器中，才能配置成功。
 * 		当在test表中插入数据是， 在audit表中可以查询到test表的变化。
 * @author Key.Xiao
 *
 */
public class TriggerTest {

	private Cluster cluster;

	private Session session;
	
	private static Properties properties = loadProperties();
	
	private static final String[] NODES = {"192.168.3.103", "192.168.3.104", "192.168.3.105"};
	
	private static final String KEYSPACE = properties.getProperty("keyspace");
	
	private static final String TABLE = properties.getProperty("table");
	
	private static final String TABLE2 = "test";
	
	public static void main(String[] args) {
		TriggerTest test = new TriggerTest();
		try {
			// 1. 连接cassandra
			test.connect(NODES);
			// 2. 创建keyspace
			test.createKeyspace();
			// 3. 创建表1：audit
			test.createTable();
			// 4. 创建表2：test
			test.createTable2();
			// 5. 创建 trigger on test表
			test.createTrigger();
			// 6. 插入数据
			test.insertTable();
			// 7. 查询数据
			test.queryData();
			// 8. 删除建空间
			test.delete();
		} finally {
			test.close();
		}
	}

	public void connect(String[] nodes) {
		for (int i = 0; i < nodes.length; i++) {
			cluster = Cluster.builder().addContactPoint(nodes[i]).build();
		}
		Metadata metadata = cluster.getMetadata();
		System.out.println("Connection to cluster: " + metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.println("Datacenter: " + host.getDatacenter() + "; Host： " + host.getAddress() + "; Rack: "
					+ host.getRack());
		}
		this.session = cluster.connect();
	}

	private void queryData() {
		String queryCql = "SELECT * FROM test.audit";
		session.execute(queryCql);
	}

	private void insertTable() {
		String insertCql = "INSERT INTO "+ KEYSPACE + "." + TABLE2 + " (key, value) values ('1', '1')";
		session.execute(insertCql);
	}

	private void createTable2() {
		String createTable = "CREATE TABLE IF NOT EXISTS "+KEYSPACE + "." + TABLE2 + " (" + "key text, " + "value text, "
				+ "PRIMARY KEY(key));";
		session.execute(createTable);
		System.out.println("create table test success!!!");
	}

	// AuditTrigger类必须build成jar包后，放入到cassandra安装环境的triggers目录下。
	private void createTrigger() {
		String createTrigger = "CREATE TRIGGER myTrigger ON "+ KEYSPACE + "." + TABLE2 +" USING 'com.key.cassandra.triggers.AuditTrigger'";
		session.execute(createTrigger);
		System.out.println("create trigger success!!!");
	}

	private void createTable() {
		String createTable = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE+ " (" + "key timeuuid, " + "keyspace_name text,"
				+ "table_name text, " + "primary_key text, " + "PRIMARY KEY(key));";
		session.execute(createTable);
		System.out.println("create table audit success!!!");
	}

	private void createKeyspace() {
		String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS "+ KEYSPACE +" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' }";
		session.execute(createKeyspace);
		System.out.println("create keyspace success!!!");

	}
	
	private static Properties loadProperties() {
		Properties properties = new Properties();
		InputStream stream = TriggerTest.class.getClassLoader().getResourceAsStream("AuditTrigger.properties");
		try {
			properties.load(stream);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			FileUtils.closeQuietly(stream);
		}
		return properties;
	}

	private void delete() {
		String deleteCql = "drop keyspace " + KEYSPACE;
		PreparedStatement insertStatement = session.prepare(deleteCql);
		BoundStatement boundStatement = new BoundStatement(insertStatement);
		session.execute(boundStatement);
	}
	
	private void close() {
		session.close();
		cluster.close();
	}
	
}