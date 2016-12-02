# Cassandra
Cassandra数据库的使用。
cassandra的本地安装（win7），和基本操作，参考博文： 
src目录下包括有：
      1. 通过连接到本地cassandra数据库，使用java语言进行简单的数据库操作。
      2. cassandra数据库的触发器功能，来源于官方demo。
lib目录下，是连接和操作cassandra数据所必须依赖的jar。 这些jar基本上都可以从cassandra安装包中得到。
doc目录下，是cassandra的运维工具：cqlsh的简单操作教程，和cassandra操作语言cql的基本语法教程。
trigger-jar目录下，是一个包含trigger功能的jar包，此jar包包含的类在src的trigger目录下。 关于如何使用trigger， 在doc目录下的Trigger.txt文件中有较详细介绍。
