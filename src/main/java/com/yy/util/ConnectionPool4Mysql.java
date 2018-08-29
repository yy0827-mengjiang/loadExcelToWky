package com.yy.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.log4j.Logger;


public class ConnectionPool4Mysql {

	private static Logger sLogger = Logger
			.getLogger(ConnectionPool4Mysql.class);

	private static ConnectionPool4Mysql pool;

	private static final String configFile = "config.mysql.properties";
	private static DataSource dataSource;

	/**
	 * Ctor.
	 */
	private ConnectionPool4Mysql() {
	}

	static {
		Properties dbProperties = new Properties();
		try {
			String configPath = PathUtil.getConfigPath(configFile);
			InputStream file = new FileInputStream(new File(configPath));
			dbProperties.load(file);
			file.close();
			dataSource = BasicDataSourceFactory.createDataSource(dbProperties);
			 
			Connection conn = getConn();
			DatabaseMetaData mdm = conn.getMetaData();
			//sLogger.info("Connected to " + mdm.getDatabaseProductName() + " "
				//	+ mdm.getDatabaseProductVersion());
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			sLogger.error("初始化连接池失败：" + e);
		}
	}

	/**
	 * 获取链接，用完后记得关闭
	 * 
	 * @see {@link DBManager#closeConn(Connection)}
	 * @return
	 */
	public static final Connection getConn() {
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			sLogger.error("获取数据库连接失败：" + e);
		}
		return conn;
	}

	/**
	 * 获取连接池实例
	 * 
	 * @return
	 */
	public static synchronized final ConnectionPool4Mysql getInstance() {
		if (pool == null) {
			try {
				pool = new ConnectionPool4Mysql();
			} catch (Exception e) {
				sLogger.error(e.getMessage());
				e.printStackTrace();
			}
		}
		return pool;
	}

	/**
	 * 执行SQL语句，支持增、删、改
	 * 
	 * @param sql
	 * @return
	 * @throws Exception
	 */
	public int execSql(String sql) throws Exception {
		int affectedRows = -1;
		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = this.getConnection();
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(sql);
			affectedRows = stmt.executeUpdate();
			conn.commit();
		} catch (Exception ex) {
			sLogger.error(sql);
			sLogger.error(ex.getMessage());
			ex.printStackTrace();
			throw new Exception("数据库错误!");
		} finally {
			free(null, stmt, conn);
		}
		return affectedRows;
	}

	/**
	 * 执行查询SQL语句
	 * 
	 * @param sql
	 * @return
	 */
	public ResultSet execSearch(String sql) {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet result = null;

		try {
			conn = this.getConnection();
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(sql.toString());
			result = stmt.executeQuery();
			return result;
		} catch (Exception ex) {
			sLogger.error(sql);
			sLogger.error(ex.getMessage());
			ex.printStackTrace();
		} finally {
			free(result, stmt, conn);
		}
		return result;
	}
	
	
	public ResultSet execSearchNoClose(String sql) {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet result = null;

		try {
			conn = this.getConnection();
			stmt = conn.prepareStatement(sql.toString());
			result = stmt.executeQuery();
			return result;
		} catch (Exception ex) {
			sLogger.error(sql);
			sLogger.error(ex.getMessage());
			ex.printStackTrace();
		}
		return result;
	}

	/**
	 * 释放资源
	 * 
	 * @param rs
	 * @param stmt
	 * @param con
	 */
	public void free(ResultSet rs, PreparedStatement stmt, Connection con) {
		try {
			if (rs != null)
				rs.close();
			if (stmt != null)
				stmt.close();
			if (con != null)
				con.close();
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * 获取数据库连接
	 * 
	 * @return
	 */
	public synchronized final Connection getConnection() {
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			sLogger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 
	 * @Title: query4Int
	 * @Description: 总数
	 * @author wuyy
	 * @date 2016年7月8日 上午10:44:49
	 *
	 * @param sql
	 * @param obj
	 * @return
	 */
	public int query4Int(String sql, Object... obj) {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet result = null;

		try {
			conn = this.getConnection();
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(sql.toString());
			if (obj != null) {
				for (int i = 0; i < obj.length; i++) {
					stmt.setObject(i + 1, obj[i]);
				}
			}
			result = stmt.executeQuery();
			while (result.next()) {
				return result.getInt(1);
			}
			return 0;
		} catch (Exception ex) {
			sLogger.error(sql);
			sLogger.error(ex.getMessage());
			ex.printStackTrace();
		} finally {
			free(result, stmt, conn);
		}
		return 0;
	}

	/**
	 * 执行语句
	 * 
	 * @Title: execBatch
	 * @Description: 执行sql PreparedStatement
	 * @author wuyy
	 * @date 2016年5月20日 上午10:25:59
	 *
	 * @param sql
	 * @param arrObj
	 */
	public boolean execBatch(String sql, List<Object[]> arrObj) {
		Connection conn = null;
		PreparedStatement pstm = null;
		try {
			conn = this.getConnection();
			conn.setAutoCommit(false);// 1,首先把Auto commit设置为false,不让它自动提交
			pstm = conn.prepareStatement(sql);
			final int batchSize = 1000;
			int count = 0;
			for (Object[] item : arrObj) {
				for (int i = 0; i < item.length; i++) {
					pstm.setObject(i + 1, item[i]);
				}
				pstm.addBatch();
				if (++count % batchSize == 0) {
					pstm.executeBatch(); // 提交一部分;
				}
			}
			pstm.executeBatch(); // 提交剩下的;
			conn.commit();// 2,进行手动提交（commit）
			conn.setAutoCommit(true);// 3,提交完成后回复现场将Auto commit,还原为true,
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			try {
				// 若出现异常，对数据库中所有已完成的操作全部撤销，则回滚到事务开始状态
				if (!conn.isClosed()) {
					conn.rollback();// 4,当异常发生执行catch中SQLException时，记得要rollback(回滚)；
					conn.setAutoCommit(true);
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			return false;
		} finally {
			free(null, pstm, conn);
		}
	}
}
