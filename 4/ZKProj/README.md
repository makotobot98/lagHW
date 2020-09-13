# README

stacks: **DBCP Connection Pool**, **ZooKeeper**, **GlassFish server**

- driver file may be found at `src/Main.java`
- flow:
    - upon server starts, 'initialize()' method will be invoked once, and will create the DB connection pool instance (in `DBUtil.java`)
    - upon creating a DB connection pool instance, `DBUtil` will register for ZK clienet and subscribe for connection information changes under a **persistent** directory `/hw3-sqlConn`
    - Client can obtain sql connection through static API `DBUtil.getDBConnection()`





# Translation
- 使用**DBCP Connection Pool**, **ZooKeeper**, **GlassFish server**
- 服务器启动时会注册一个连接池，并通过zk客户端subscribe `/hw3-sqlConn`目录下的连接信息，如有数据变更，则会创建新的连接池