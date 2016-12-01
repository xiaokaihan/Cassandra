package com.key.cassandra.demo.trigger;

import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

/**
 * 该类实现ITrigger接口 , 根据配置自定义Trigger。
 * 
 * AuditTrigger类的作用是： 
 *  The AuditTrigger class will create a basic audit of activity on a table.
 * 	在table1中配置一个trigger，使得该table1与Trigger中绑定的table2相关联；
 *  当该table1中的数据发生变化时，可以在table2看到table1中的变化。
 * 
 *  将该类和相对应的配置文件build成一个jar包， 放入到cassandra安装目录的triggers目录下。 
 *  jar包放到： cassandra机器上的 /etc/dse/cassandra/triggers 目录下。
 *  配置文件放到： jar包放到： cassandra机器上的 /etc/dse/cassandra 目录下。 
 *  来自 cassandra 示例代码
 *
 */
public class AuditTrigger implements ITrigger {
	
	private Properties properties = loadProperties();

	public Collection<Mutation> augment(Partition update) {
		String auditKeyspace = properties.getProperty("keyspace");
		String auditTable = properties.getProperty("table");
		System.out.println(auditKeyspace);
		System.out.println(auditKeyspace);

		RowUpdateBuilder audit = new RowUpdateBuilder(Schema.instance.getCFMetaData(auditKeyspace, auditTable),
				FBUtilities.timestampMicros(), UUIDGen.getTimeUUID());

		audit.add("keyspace_name", update.metadata().ksName);
		audit.add("table_name", update.metadata().cfName);
		audit.add("primary_key", update.metadata().getKeyValidator().getString(update.partitionKey().getKey()));

		return Collections.singletonList(audit.build());
	}

	private static Properties loadProperties() {
		Properties properties = new Properties();
		InputStream stream = AuditTrigger.class.getClassLoader().getResourceAsStream("AuditTrigger.properties");
		try {
			properties.load(stream);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			FileUtils.closeQuietly(stream);
		}
		return properties;
	}

}