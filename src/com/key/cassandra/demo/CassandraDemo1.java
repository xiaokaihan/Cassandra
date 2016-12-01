package com.key.cassandra.demo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Properties;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * 使用java连接到cassandra， 对cassandra进行简单的增删查改操作。
 * 
 * @author Key.Xiao
 * @version 1.0
 * @since 2016年11月4日18:36:43
 */
public class CassandraDemo1 {

	Properties properties = new Properties();

	Session session;
	BoundStatement boundStatement;

	public CassandraDemo1() {
		// load a properties file
		try {
			properties.load(new FileInputStream("conf/importBinary.properties"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String keyspace = properties.getProperty("keyspace");
		// connect to Cassandra cluster and create session
		Cluster cluster = Cluster.builder().addContactPoint("192.168.3.103").addContactPoint("192.168.3.104")
				.addContactPoint("192.168.3.105").build();
		System.out.println("Connected to cluster: " + cluster.getMetadata().getClusterName());
		// Recommend a keyspace corresponding to a session.
		// cluster.connect(): for all keyspace.
		session = cluster.connect(keyspace);

		// drop table if exists. (once only)
		session.execute("drop table if exists " + keyspace + ".ticker");

		// create table(column family). (once only)
		session.execute("create table " + keyspace + ".ticker (" + "date text," + "stock_code bigint,"
				+ "timestamp timestamp," + "ticker_time timestamp," + "cancel_flag text," + "price decimal,"
				+ "ticker_id bigint," + "ticker_type bigint," + "volume bigint," + "sell boolean,"
				+ "bid_price decimal," + "ask_price decimal," + "side text,"
				+ "primary key ((date, stock_code), ticker_time, ticker_id))");

		/*
		 * String sqlGPS = "CREATE TABLE IF NOT EXISTS hkex_test.gps (" +
		 * "time varchar," + "gps_time varchar,longitude varchar," +
		 * "latitude varchar,altitude varchar," +
		 * "heading varchar,speed varchar," +
		 * "source varchar,satellites varchar," + "PRIMARY KEY (time))";
		 * session.execute(sqlGPS);
		 */

		// 插入数据cql的预处理
		boundStatement = new BoundStatement(session.prepare(
				"insert into " + keyspace + ".ticker (" + "date," + "stock_code," + "timestamp," + "ticker_time,"
						+ "cancel_flag," + "price," + "ticker_id," + "ticker_type," + "volume," + "sell," + "bid_price,"
						+ "ask_price," + "side" + ") values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"));
		String date = "2016-11-03";
		String cancel_flag = "ticker";
		long securityCode = 1;
		long time = System.currentTimeMillis();
		long price = 1000;
		long ticker_getTrdType_recommend = 123456;
		long aggregateQuantity = 456789;
		long[] bidPrice = new long[1000];
		long[] askPrice = new long[1000];
		int i = 1;
		String side = "sell";
		// 执行插入值
		session.execute(boundStatement.bind(date, securityCode, new Date(time), new Date(time * 10), cancel_flag,
				decimal(price), ticker_getTrdType_recommend, Long.valueOf(ticker_getTrdType_recommend),
				aggregateQuantity, (price <= bidPrice[i]), decimal(bidPrice[i]), decimal(askPrice[i]), side));

		System.out.println("inter ticker success!!!");
		// select table
		String selectSql = "select * from hkex_test.ticker";
		// return result set
		ResultSet resultSet = session.execute(selectSql);
		int count = 0;
		// traversal result set and print.
		for (Row row : resultSet) {
			System.out.println("date: " + row.getString("date"));
			System.out.println("cancel_flag: " + row.getString("cancel_flag"));
			System.out.println("volume: " + row.getLong("volume"));
			count++;
		}
		System.err.println("行数:" + count);
		
		//TODO create secondary index
		
		//TODO  drop table
		
	}

	private BigDecimal decimal(long d) {
		return (new BigDecimal(d)).divide(new BigDecimal(1000));
	}

	public static void main(String[] args) {
		new CassandraDemo1();
	}

}