package com.key.cassandra.demo;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * 使用java连接到cassandra数据库， 进行增删改查操作。
 * 
 * 1. createKsAndCf(); 创建指定名字的keyspace和table
 * 2. insertData(int num); 插入指定数量的数据
 * 3. loadData(); 查询数据
 * 4. updateData(People people); 更新指定行
 * 5. client.deleteData(int[] ids);  删除指定的行
 * 
 * @author Key.Xiao
 *
 */
public class CassandraDemo {

	private Cluster cluster;

	private Session session;
	
	private static final String[] NODES = {"192.168.3.103", "192.168.3.104", "192.168.3.105"};
	
	private static final String KEYSPACE = "keysdemo";
	
	private static final String TABLENAME = "users";
	
	public static void main(String[] args) {
		CassandraDemo client = new CassandraDemo();
		client.connect(NODES);
		try {
			client.createKsAndCf();
			client.insertData(10);
			client.loadData();
			// 更新指定id的数据
			People people = new People(1, "fname", "lname", "male", 23, "xyz@gmial.com");
			client.updateData(people);
			client.loadData();
			// 删除指定id的数据
			int[] ids = {1,2,3,4,5};
			client.deleteData(ids);
			System.out.println("删除后的数据");
			client.loadData();
			// 删除表和键空间
//			client.dropTable();
//			client.dropKeyspace();
		} finally {
			// always execute close;
			client.close();
		}
	}

	/**
	 * 连接节点
	 * 
	 * @param nodes
	 *            节点数组
	 */
	public void connect(String[] nodes) {
		for (int i = 0; i < nodes.length; i++) {
			cluster = Cluster.builder().addContactPoint(nodes[i]).build();
		}
		Metadata metadata = cluster.getMetadata();
		System.out.println("Connection to cluster: " + metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.println("Datacenter: " + host.getDatacenter() +
					"; Host： " + host.getAddress() + "; Rack: " + host.getRack());
		}
		this.session = cluster.connect();
	}

	/**
	 * create specific keyspace and tableName
	 * 
	 * @param keyspace
	 * @param tableName
	 */
	public void createKsAndCf() {
		getSession().execute("drop keyspace if exists " + KEYSPACE);
		// create keyspace
		/*String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS " + keyspace
				+ " WITH REPLICATION = {'class': 'SimpleStrategy','replication_factor':1}";*/

		// 推荐使用 ，多个数据中心（节点）使用NetWorkTopologyStrategy; replication_factor不能超过数据节点数，一般是刚好等于数据节点的个数，实现每个节点都有一份数据备份。
		// durable_writes参数默认为true，可以不用设置
		String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE
				+ " WITH REPLICATION = {'class': 'SimpleStrategy','replication_factor':1} And durable_writes = true" ;
		getSession().execute(createKeyspace);
		getSession().execute("drop table if exists " + KEYSPACE + "." + TABLENAME);
		// create table
		String createTable = "CREATE TABLE IF NOT EXISTS " +  KEYSPACE + "." + TABLENAME + " (" 
				+ "id int," + "fname varchar,"
				+ "lname varchar," 
				+ "gender varchar," 
				+ "age int," 
				+ "email varchar," 
				+ "PRIMARY KEY (id))";
		getSession().execute(createTable);
	}

	/**
	 * insert data. for test multi data
	 * 
	 * @param count
	 *            insert count
	 */
	public void insertData(int count) {
		String insertCql = "INSERT INTO " + KEYSPACE +"."+ TABLENAME + "(id, fname, lname, gender, age, email)"
				+ "VALUES(?, ?, ?, ?, ?, ?)";
		PreparedStatement insertStatement = getSession().prepare(insertCql);
		BoundStatement boundStatement = new BoundStatement(insertStatement);
		// execute insert
		for (int i = 1; i <= count; i++) {
			int id = i;
			String fname = "key" + i;
			String lname = "xiao" + i;
			String gender = "male";
			int age = 20 + i;
			String email = "dreamkey" + i + "@gmail.com";
			getSession().execute(boundStatement.bind(id, fname, lname, gender, age, email));
		}
	}

	/**
	 * pass a specific object.
	 */
	public void insertData() {
		String insertCql = "INSERT INTO keysdemo.users" + "(id, fname, lname, gender, age, email)"
				+ "VALUES(?, ?, ?, ?, ?, ?)";
		PreparedStatement insertStatement = getSession().prepare(insertCql);
		BoundStatement boundStatement = new BoundStatement(insertStatement);
		// execute insert
		int id = 1;
		String fname = "key" + 1;
		String lname = "xiao" + 1;
		String gender = "male";
		int age = 20 + 1;
		String email = "dreamkey" + 1 + "@gmail.com";
		getSession().execute(boundStatement.bind(id, fname, lname, gender, age, email));
	}

	/**
	 * query specific data
	 */
	public void loadData(){
		String queryCql = "select * from "+ KEYSPACE + "." + TABLENAME;
		int count = 0;
		ResultSet resultSet = getSession().execute(queryCql);
		for (Row row : resultSet) {
			System.out.println(
					"id: " + row.getInt("id") + 
					"\t fname: "+ row.getString("fname") + 
					"\t lname" + row.getString("lname") +
					"\t gender: " + row.getString("gender") + 
					"\t age: " + row.getInt("age") + 
					"\t email: " + row.getString("email"));
			count++;
		}
		System.err.println("一共： " + count + " 条数据");
	}
	
	/**
	 * delete specific data.
	 */
	public void deleteData(int[] id){
		for (int i = 0; i < id.length; i++) {
			String delCql = "delete from " + KEYSPACE + "." + TABLENAME + " where id = " + id[i];
			getSession().execute(delCql);
		}
	}
	
	/**
	 * update specific data.
	 * @param id
	 * @param age
	 */
	public void updateData(People people){
		int id = people.getId();
		String fname = people.getFname();
		String lname = people.getLname();
		String gender = people.getGender();
		int age = people.getAge();
		String email = people.getEmail();
		String updateCql = "update " + KEYSPACE + "." + TABLENAME + " set "
					+ "fname = '" + fname 
					+ "',lname = '" + lname 
					+ "',gender = '" + gender 
					+ "',age = " + age 
					+ ",email = '" + email 
					+ "' where id = " + id;
		getSession().execute(updateCql);
	}
	
	/**
	 * 删除表 
	 */
	public void dropTable(){
		String dropTable = "drop COLUMNFAMILY " + KEYSPACE + "." + TABLENAME;
		getSession().execute(dropTable);
	}
	
	/**
	 * 删除keyspace
	 */
	public void dropKeyspace(){
		String dropKeyspace = "drop keyspace " + KEYSPACE;
		getSession().execute(dropKeyspace);
	}
	
	public Cluster getCluster() {
		return this.cluster;
	}

	public Session getSession() {
		return this.session;
	}
	
	public void close(){
		this.session.close();
		this.cluster.close();
		System.out.println("close session!!!");
	}
}