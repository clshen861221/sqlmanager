package org.clshen.sqlmanager;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.commons.beanutils.BasicDynaClass;
import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.beanutils.DynaClass;
import org.apache.commons.beanutils.DynaProperty;
import org.apache.commons.dbcp.BasicDataSource;

public class SQLManager {

	public static List<Object> searchEntity(String sSQL, List<Object> params,
			Connection oSQLConnection, Class<?> clazz) throws Exception {
		List<Object> objects = new ArrayList<Object>();

		PreparedStatement ps = null;

		ResultSet rs = null;

		try {
			ps = oSQLConnection.prepareStatement(sSQL);

			if (params != null && params.size() > 0) {
				for (int i = 0; i < params.size(); i++) {
					ps.setObject(i + 1, params.get(i));
				}
			}

			rs = ps.executeQuery();

			while (rs.next()) {

				Object obj = clazz.newInstance();

				ResultSetMetaData rsm = rs.getMetaData();

				for (int i = 0; i < rsm.getColumnCount(); i++) {
					String columnName = rsm.getColumnName(i + 1);

					Field[] fields = clazz.getDeclaredFields();

					for (int j = 0; j < fields.length; j++) {
						String fieldName = fields[j].getName();
						if (fieldName.toUpperCase().equals(
								columnName.toUpperCase())) {
							Method method = null;
							try {
								method = clazz.getMethod(
										"set"
												+ fieldName.substring(0, 1)
														.toUpperCase()
												+ fieldName.substring(1),
										fields[j].getType());

								method.invoke(obj, rs.getObject(columnName));
							} catch (Exception e) {
								method = null;
							}

							break;
						}
					}
				}

				objects.add(obj);

			}

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				try {
					rs.close();
					rs = null;
				} catch (SQLException e) {
					throw e;
				}
			}

			if (ps != null) {
				try {
					ps.close();
					ps = null;
				} catch (SQLException e) {
					throw e;
				}
			}
		}

		return objects;
	}

	public static void executeSql(String sSQL, Connection oSQLConnection,
			List<Object> params) throws Exception {
		PreparedStatement ps = null;

		try {
			ps = oSQLConnection.prepareStatement(sSQL);

			if (params != null && params.size() > 0) {
				for (int i = 0; i < params.size(); i++) {
					ps.setObject(i + 1, params.get(i));
				}
			}

			ps.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {

			if (ps != null) {
				try {
					ps.close();
					ps = null;
				} catch (SQLException e) {
					e.printStackTrace();
					throw e;
				}
			}
		}

	}

	public static List<List<Object>> executeQuery(String sSQL,
			Connection oSQLConnection, List<Object> params) throws Exception {
		List<List<Object>> records = new ArrayList<List<Object>>();

		List<Object> columns = new ArrayList<Object>();

		PreparedStatement ps = null;

		ResultSet rs = null;

		try {

			ps = oSQLConnection.prepareStatement(sSQL);

			if (params != null && params.size() > 0) {
				for (int i = 0; i < params.size(); i++) {
					ps.setObject(i + 1, params.get(i));
				}
			}

			rs = ps.executeQuery();

			ResultSetMetaData metaData = rs.getMetaData();

			Integer colNum = metaData.getColumnCount();

			while (rs.next()) {
				columns.clear();
				for (int i = 1; i <= colNum; i++) {
					columns.add(rs.getObject(i));
				}
				records.add(columns);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				try {
					rs.close();
					rs = null;
				} catch (SQLException e) {
					throw e;
				}
			}

			if (ps != null) {
				try {
					ps.close();
					ps = null;
				} catch (SQLException e) {
					throw e;
				}
			}
		}

		return records;
	}

	public static String tableColumnTool(String tableName, Connection conn)
			throws Exception {
		String sReturn = "";

		String sql = "SELECT * FROM " + tableName + " WHERE 1=2";

		Statement st = null;

		ResultSet rs = null;

		try {
			st = conn.createStatement();

			rs = st.executeQuery(sql);

			ResultSetMetaData rsmd = rs.getMetaData();

			int columnNum = rsmd.getColumnCount();

			for (int i = 1; i <= columnNum; i++) {
				String columnName = rsmd.getColumnName(i).toLowerCase();

				String columnCatalog = rsmd.getColumnClassName(i);

				String columnTCatalogName = columnCatalog
						.substring(columnCatalog.lastIndexOf(".") + 1);

				sReturn = sReturn + "private " + columnTCatalogName + " "
						+ columnName + "; \n \n";

			}

		} catch (Exception e) {
			e.printStackTrace();

			throw e;
		} finally {
			if (rs != null) {
				rs.close();
				rs = null;
			}

			if (st != null) {
				st.close();
				st = null;
			}
		}

		return sReturn;
	}

	@SuppressWarnings("unchecked")
	public static void saveEntity(Object entity, String tableName,
			Connection conn) throws Exception {

		if (entity == null || tableName == null || "".equals(tableName.trim())) {
			return;
		}

		List<String> fieldNameList = new ArrayList<String>();

		List<Object> fieldValueList = new ArrayList<Object>();

		Class clazz = entity.getClass();

		tableName = tableName.trim().toUpperCase();

		String insertSql = "INSERT INTO " + tableName + "(";

		PreparedStatement ps = null;

		try {

			Field[] fields = clazz.getDeclaredFields();

			Object fieldValue = null;

			Method method = null;

			for (int j = 0; j < fields.length; j++) {
				String fieldName = fields[j].getName();

				try {
					method = clazz.getMethod("get"
							+ fieldName.substring(0, 1).toUpperCase()
							+ fieldName.substring(1));

					fieldValue = method.invoke(entity);

				} catch (Exception e) {
					method = null;
				}

				if (method == null || fieldValue == null
						|| !isBasicType(fieldValue.getClass())) {
					continue;
				}

				if (fieldValue instanceof Boolean) {
					Boolean bol = (Boolean) fieldValue;

					if (bol) {
						fieldValue = 1;
					} else {
						fieldValue = 0;
					}
				}

				fieldNameList.add(fieldName);

				fieldValueList.add(fieldValue);

			}

			if (fieldNameList == null || fieldNameList.size() == 0) {
				insertSql = null;
			} else {
				for (String fieldName : fieldNameList) {
					insertSql = insertSql + fieldName + ",";
				}

				insertSql = insertSql.substring(0, insertSql.length() - 1)
						+ ") VALUES(";

				for (int i = 0; i < fieldValueList.size(); i++) {
					insertSql = insertSql + "?,";
				}

				insertSql = insertSql.substring(0, insertSql.length() - 1)
						+ ")";

				ps = conn.prepareStatement(insertSql);

				System.out.println(insertSql);

				for (int i = 0; i < fieldValueList.size(); i++) {
					Object value = fieldValueList.get(i);

					ps.setObject(i + 1, value);
				}

				ps.executeUpdate();

			}

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null) {
				ps.close();
				ps = null;
			}
		}

	}

	@SuppressWarnings("unchecked")
	public static Object updateEntity(Object entity, String tableName,
			Connection conn) throws Exception {
		Object obj = null;

		if (entity == null || tableName == null || "".equals(tableName.trim())) {
			return null;
		}

		List<String> fieldNameList = new ArrayList<String>();

		List<Object> fieldValueList = new ArrayList<Object>();

		List<Object> updatedFieldValueList = new ArrayList<Object>();

		Class clazz = entity.getClass();

		tableName = tableName.trim().toUpperCase();

		String updateSql = "UPDATE " + tableName + " SET ";

		ResultSet pkRs = null;

		PreparedStatement ps = null;

		String primaryKeyColumnName = null;

		Object primaryKeyValue = null;

		try {

			DatabaseMetaData dmd = conn.getMetaData();

			pkRs = dmd.getPrimaryKeys(null, null, tableName);

			while (pkRs.next()) {
				primaryKeyColumnName = pkRs.getString("COLUMN_NAME");
			}

			if (primaryKeyColumnName != null) {
				Field[] fields = clazz.getDeclaredFields();
				Method method = null;
				Object fieldValue = null;
				for (int j = 0; j < fields.length; j++) {
					String fieldName = fields[j].getName();

					try {
						method = clazz.getMethod("get"
								+ fieldName.substring(0, 1).toUpperCase()
								+ fieldName.substring(1));

						fieldValue = method.invoke(entity);

					} catch (Exception e) {
						method = null;
					}

					if (method == null) {
						continue;
					}

					if (fieldName.toUpperCase().equals(
							primaryKeyColumnName.toUpperCase())) {
						primaryKeyValue = fieldValue;

						continue;
					}

					if (!isBasicType(fields[j].getType())) {
						continue;
					}

					if (fieldValue instanceof Boolean) {
						Boolean bol = (Boolean) fieldValue;

						if (bol) {
							fieldValue = 1;
						} else {
							fieldValue = 0;
						}
					}

					fieldNameList.add(fieldName);

					fieldValueList.add(fieldValue);

				}

				if (fieldNameList == null || fieldNameList.size() == 0) {
					updateSql = null;
				} else {
					for (int i = 0; i < fieldNameList.size(); i++) {
						String columName = fieldNameList.get(i);

						Object value = fieldValueList.get(i);

						if (value == null) {
							updateSql = updateSql + columName + "=null, ";

						} else {
							updateSql = updateSql + columName + "=?, ";
						}
					}

					updateSql = updateSql.substring(0, updateSql.length() - 2)
							+ " WHERE " + primaryKeyColumnName + "=?";

					System.out.println(updateSql);

					ps = conn.prepareStatement(updateSql);

					if (primaryKeyValue == null) {
						return null;
					} else {
						fieldValueList.add(primaryKeyValue);

						for (int i = 0; i < fieldValueList.size(); i++) {
							Object o = fieldValueList.get(i);

							if (o != null) {
								updatedFieldValueList.add(o);
							}
						}

						for (int i = 0; i < updatedFieldValueList.size(); i++) {
							Object value = updatedFieldValueList.get(i);

							ps.setObject(i + 1, value);

						}

						ps.executeUpdate();

						obj = findEntityByIdentity(primaryKeyValue, tableName,
								clazz, conn);
					}

				}

				return obj;

			} else {
				return null;
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null) {
				ps.close();
				ps = null;
			}

			if (pkRs != null) {
				pkRs.close();
				pkRs = null;
			}
		}
	}

	public static Object findEntityByIdentity(Object id, String tableName,
			Class<?> clazz, Connection conn) throws Exception {
		Object obj = null;

		if (id == null) {
			return obj;
		}

		tableName = tableName.trim().toUpperCase();

		String getEntitySql = "SELECT * FROM " + tableName + " WHERE ";

		ResultSet pkRs = null;

		PreparedStatement entityPs = null;

		ResultSet entityRs = null;

		String primaryKeyColumnName = null;

		try {

			DatabaseMetaData dmd = conn.getMetaData();

			pkRs = dmd.getPrimaryKeys(null, null, tableName);

			while (pkRs.next()) {
				primaryKeyColumnName = pkRs.getString("COLUMN_NAME");
			}

			if (primaryKeyColumnName != null) {
				getEntitySql = getEntitySql + primaryKeyColumnName + "=?";

				entityPs = conn.prepareStatement(getEntitySql);

				entityPs.setObject(1, id);

				entityRs = entityPs.executeQuery();

				while (entityRs.next()) {
					obj = clazz.newInstance();

					ResultSetMetaData rsm = entityRs.getMetaData();

					for (int i = 0; i < rsm.getColumnCount(); i++) {
						String columnName = rsm.getColumnName(i + 1);

						Field[] fields = clazz.getDeclaredFields();

						for (int j = 0; j < fields.length; j++) {
							String fieldName = fields[j].getName();
							if (fieldName.toUpperCase().equals(
									columnName.toUpperCase())) {
								Method method = null;

								try {
									method = clazz.getMethod(
											"set"
													+ fieldName.substring(0, 1)
															.toUpperCase()
													+ fieldName.substring(1),
											fields[j].getType());

									method.invoke(obj,
											entityRs.getObject(columnName));
								} catch (Exception e) {
									method = null;
								}

								break;
							}
						}
					}
				}
			}

		} catch (Exception e) {
			throw e;
		} finally {
			if (entityRs != null) {
				entityRs.close();
				entityRs = null;
			}

			if (entityPs != null) {
				entityPs.close();
				entityPs = null;
			}

			if (pkRs != null) {
				pkRs.close();
				pkRs = null;
			}
		}

		return obj;
	}

	public static void insertBatch(Connection conn, List<?> list,
			String identityColumnName, boolean isAutoIdentity, String tableName)
			throws Exception {
		if (list == null || list.size() == 0) {
			return;
		}

		Object obj = list.get(0);
		Class<?> clazz = obj.getClass();
		Field[] fields = clazz.getDeclaredFields();

		List<String> replaceList = new ArrayList<String>();
		StringBuffer sql = new StringBuffer("INSERT INTO " + tableName + " (");
		Field field = null;
		String fieldName = null;
		Object fieldValue = null;
		Method method = null;
		List<Field> fieldList = new ArrayList<Field>();
		for (int i = 0; i < fields.length; i++) {
			field = fields[i];
			if (isBasicType(field.getType())) {
				fieldList.add(field);
			}
		}

		for (int i = 0; i < fieldList.size(); i++) {
			field = fieldList.get(i);
			fieldName = field.getName();

			try {
				method = clazz.getMethod("get"
						+ fieldName.substring(0, 1).toUpperCase()
						+ fieldName.substring(1));
			} catch (Exception e) {
				method = null;
			}

			if (method == null) {
				continue;
			}

			if (isAutoIdentity) {
				if (identityColumnName != null
						&& field.getName().toUpperCase()
								.equals(identityColumnName.toUpperCase())) {
					continue;
				}
			}

			if (!isBasicType(field.getType())) {
				continue;
			}
			if (i == fieldList.size() - 1) {
				sql.append(field.getName() + ") VALUES (");
				replaceList.add("?)");
			} else {
				sql.append(field.getName() + ",");
				replaceList.add("?,");
			}

		}
		for (int i = 0; i < replaceList.size(); i++) {
			sql.append(replaceList.get(i));
		}

		System.out.println("Batch Insert:" + sql.toString());

		PreparedStatement ps = null;
		Class<?> type = null;
		int index = 0;
		try {
			ps = conn.prepareStatement(sql.toString());

			for (Object bean : list) {
				for (int i = 0; i < fieldList.size(); i++) {
					field = fieldList.get(i);
					fieldName = field.getName();
					if (isAutoIdentity) {
						if (identityColumnName != null
								&& fieldName.toUpperCase().equals(
										identityColumnName.toUpperCase())) {
							continue;
						}
					}

					try {
						method = clazz.getMethod("get"
								+ fieldName.substring(0, 1).toUpperCase()
								+ fieldName.substring(1));
						fieldValue = method.invoke(bean);
					} catch (Exception e) {
						method = null;
					}

					if (method == null) {
						continue;
					}

					type = field.getType();

					if (fieldValue == null) {
						ps.setNull(index + 1, getJDBCType(type));
					} else {
						ps.setObject(index + 1, fieldValue);
					}

					index++;
				}

				index = 0;

				ps.addBatch();
			}
			ps.executeBatch();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null) {
				ps.close();
				ps = null;
			}
		}

	}

	public static void updateBatch(Connection conn, List<?> list,
			String tableName) throws Exception {
		if (list == null || list.size() == 0 || tableName == null
				|| "".equals(tableName.trim())) {
			return;
		}
		Object entity = list.get(0);
		List<String> fieldNameList = new ArrayList<String>();
		List<Object> fieldValueList = new ArrayList<Object>();
		Class<?> clazz = entity.getClass();
		tableName = tableName.trim().toUpperCase();
		ResultSet pkRs = null;
		String primaryKeyColumnName = null;
		Object primaryKeyValue = null;
		PreparedStatement ps = null;
		Field field = null;
		String fieldName = null;
		Method method = null;
		Object fieldValue = null;
		Class<?> type = null;
		try {
			DatabaseMetaData dmd = conn.getMetaData();
			pkRs = dmd.getPrimaryKeys(null, null, tableName);

			while (pkRs.next()) {
				primaryKeyColumnName = pkRs.getString("COLUMN_NAME");
			}

			Field[] fields = clazz.getDeclaredFields();

			for (int j = 0; j < fields.length; j++) {
				field = fields[j];
				fieldName = field.getName();
				type = field.getType();
				if (!isBasicType(type)) {
					continue;
				}

				if (fieldName.toUpperCase().equals(
						primaryKeyColumnName.toUpperCase())) {
					continue;
				}
				fieldNameList.add(fieldName);

			}

			StringBuffer updateSql = new StringBuffer("UPDATE " + tableName
					+ " SET ");
			if (fieldNameList == null || fieldNameList.size() == 0) {
				updateSql = null;
			} else {
				for (int i = 0; i < fieldNameList.size(); i++) {
					String columName = fieldNameList.get(i);
					updateSql.append(columName + "=?, ");

				}

				updateSql = new StringBuffer(updateSql.toString().substring(0,
						updateSql.length() - 2));
				updateSql.append(" WHERE " + primaryKeyColumnName + "=?");
				System.out.println("Batch update:" + updateSql.toString());

			}

			if (updateSql != null) {
				ps = conn.prepareStatement(updateSql.toString());
				for (Object obj : list) {
					fieldValueList.clear();
					fieldValueList = new ArrayList<Object>();
					for (int i = 0; i < fields.length; i++) {
						field = fields[i];
						fieldName = field.getName();
						type = field.getType();
						try {
							method = clazz.getMethod("get"
									+ fieldName.substring(0, 1).toUpperCase()
									+ fieldName.substring(1));
							fieldValue = method.invoke(obj);

							if (fieldName.toUpperCase().equals(
									primaryKeyColumnName.toUpperCase())) {
								primaryKeyValue = fieldValue;
								continue;
							}

							if (fieldValue == null) {
								Empty empty = new Empty();
								empty.setJdbcType(getJDBCType(type));
								fieldValue = empty;
							}

							if (fieldValue instanceof Boolean) {
								Boolean bol = (Boolean) fieldValue;
								if (bol) {
									fieldValue = 1;
								} else {
									fieldValue = 0;
								}
							}

							fieldValueList.add(fieldValue);
						} catch (Exception e) {
							method = null;
						}
					}

					if (primaryKeyValue == null) {
						return;
					} else {
						fieldValueList.add(primaryKeyValue);

						for (int i = 0; i < fieldValueList.size(); i++) {
							Object value = fieldValueList.get(i);
							if (value instanceof Empty) {
								Empty empty = (Empty) value;
								ps.setNull(i + 1, empty.getJdbcType());
							} else {
								ps.setObject(i + 1, value);
							}

						}

						ps.addBatch();

					}

				}

				ps.executeBatch();
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null) {
				ps.close();
				ps = null;
			}
			if (pkRs != null) {
				pkRs.close();
				pkRs = null;
			}
		}

	}

	private static Integer getJDBCType(Class<?> clazz) {
		Integer type = null;

		if (clazz.equals(Long.class)) {
			type = Types.BIGINT;
		} else if (clazz.equals(Byte.class)) {
			type = Types.BINARY;
		} else if (clazz.equals(Boolean.class)) {
			type = Types.BIT;
		} else if (clazz.equals(String.class)) {
			type = Types.CHAR;
		} else if (clazz.equals(Date.class)) {
			type = Types.DATE;
		} else if (clazz.equals(BigDecimal.class)) {
			type = Types.DECIMAL;
		} else if (clazz.equals(Double.class)) {
			type = Types.FLOAT;
		} else if (clazz.equals(Integer.class)) {
			type = Types.INTEGER;
		} else if (clazz.equals(Short.class)) {
			type = Types.SMALLINT;
		} else if (clazz.equals(Timestamp.class)) {
			type = Types.TIMESTAMP;
		}

		return type;
	}

	public static boolean isBasicType(Class<?> clazz) {
		boolean isBasicType = false;
		if (clazz.equals(Long.class)) {
			isBasicType = true;
		} else if (clazz.equals(Byte.class)) {
			isBasicType = true;
		} else if (clazz.equals(Boolean.class)) {
			isBasicType = true;
		} else if (clazz.equals(String.class)) {
			isBasicType = true;
		} else if (clazz.equals(Date.class)) {
			isBasicType = true;
		} else if (clazz.equals(BigDecimal.class)) {
			isBasicType = true;
		} else if (clazz.equals(Double.class)) {
			isBasicType = true;
		} else if (clazz.equals(Integer.class)) {
			isBasicType = true;
		} else if (clazz.equals(Short.class)) {
			isBasicType = true;
		} else if (clazz.equals(Timestamp.class)) {
			isBasicType = true;
		}

		return isBasicType;

	}

	private static class Empty {
		private int jdbcType;

		public int getJdbcType() {
			return jdbcType;
		}

		public void setJdbcType(int jdbcType) {
			this.jdbcType = jdbcType;
		}
	}

	private static final String BUNDLE_NAME = "db"; //$NON-NLS-1$

	private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle
			.getBundle(BUNDLE_NAME);

	private static BasicDataSource oDS = null;

	@SuppressWarnings("deprecation")
	public static Connection getConnection() throws SQLException,
			ClassNotFoundException {
		if (oDS == null) {
			Class.forName(getString("DriverClass"));
			oDS = new BasicDataSource();
			oDS.setDriverClassName(getString("DriverClass"));
			oDS.setUsername(getString("Username"));
			oDS.setPassword(getString("Password"));
			oDS.addConnectionProperty("oracle.jdbc.V8Compatible", "true");
			oDS.setUrl(getString("URL"));
			oDS.setMaxActive(200);
			oDS.setMaxIdle(10);
			oDS.setMaxWait(60000);
			oDS.setTestOnBorrow(true);
			oDS.setRemoveAbandoned(true);
			oDS.setRemoveAbandonedTimeout(60);
			oDS.setLogAbandoned(true);

		}

		return oDS.getConnection();
	}

	public static String getString(String key) {
		try {
			return RESOURCE_BUNDLE.getString(key);
		} catch (MissingResourceException e) {
			return '!' + key + '!';
		}
	}

	public static List<Object> searchDeepEntity(String sSQL,
			List<Object> params, Connection oSQLConnection, Class<?> clazz)
			throws Exception {
		List<Object> objects = new ArrayList<Object>();

		PreparedStatement ps = null;

		ResultSet rs = null;

		try {
			ps = oSQLConnection.prepareStatement(sSQL);

			if (params != null && params.size() > 0) {
				for (int i = 0; i < params.size(); i++) {
					ps.setObject(i + 1, params.get(i));
				}
			}

			rs = ps.executeQuery();
			Entity entity = clazz.getAnnotation(Entity.class);
			String primaryKey = entity.primaryKey();
			Object obj = null;
			ResultSetMetaData rsm = null;
			DynaProperty[] properties = null;
			DynaClass dynaClass = null;
			DynaBean bean = null;
			Field[] fields = null;
			Field field = null;
			String columnName = null;
			Object dynaBeanPrimaryKeyValue = null;
			Object propertyValue = null;
			Object subBeanPropertyValue = null;
			String columnTypeName = null;
			String columnClassName = null;

			rsm = rs.getMetaData();
			properties = new DynaProperty[rsm.getColumnCount()];
			for (int i = 0; i < rsm.getColumnCount(); i++) {
				columnName = rsm.getColumnName(i + 1);
				columnTypeName = rsm.getColumnTypeName(i + 1);
				columnClassName = rsm.getColumnClassName(i + 1);
				if ("NUMBER".equals(columnTypeName)) {
					columnClassName = "java.math.BigDecimal";
				}

				properties[i] = new DynaProperty(columnName,
						Class.forName(columnClassName));
			}
			dynaClass = new BasicDynaClass("dynaBean", null, properties);

			while (rs.next()) {
				bean = dynaClass.newInstance();
				for (int i = 0; i < rsm.getColumnCount(); i++) {
					columnName = rsm.getColumnName(i + 1);
					bean.set(columnName, rs.getObject(columnName));
				}
				boolean isAdd = true;

				if (primaryKey != null) {
					try {
						dynaBeanPrimaryKeyValue = bean.get(primaryKey
								.toUpperCase());
					} catch (Exception e) {
						dynaBeanPrimaryKeyValue = null;
					}

					if (dynaBeanPrimaryKeyValue != null) {
						for (Object obj1 : objects) {
							Method getPrimayKeyMethod = clazz.getMethod("get"
									+ primaryKey.substring(0, 1).toUpperCase()
									+ primaryKey.substring(1));
							Object primaryKeyValue = getPrimayKeyMethod
									.invoke(obj1);
							if (primaryKeyValue.equals(dynaBeanPrimaryKeyValue)) {
								obj = obj1;
								isAdd = false;
								break;
							}
						}
					}

				}

				if (isAdd) {
					obj = clazz.newInstance();
				}

				fields = clazz.getDeclaredFields();
				for (int i = 0; i < fields.length; i++) {
					field = fields[i];
					String fieldName = field.getName();
					if (isAdd) {
						Transient oTransient = field
								.getAnnotation(Transient.class);
						if (oTransient == null) {
							try {
								propertyValue = bean.get(fieldName
										.toUpperCase());
							} catch (Exception e) {
								propertyValue = null;
							}
							if (propertyValue != null) {
								Method method = clazz.getMethod(
										"set"
												+ fieldName.substring(0, 1)
														.toUpperCase()
												+ fieldName.substring(1), field
												.getType());
								method.invoke(obj, propertyValue);
							}
						}

					}

					OneToMany oneToMany = field.getAnnotation(OneToMany.class);
					if (oneToMany != null) {
						String subBeanClazzName = oneToMany.bean();
						Class<?> subBeanClazz = Class.forName(subBeanClazzName
								.trim());
						Entity subEntity = subBeanClazz
								.getAnnotation(Entity.class);
						String subPrimaryKey = subEntity.primaryKey();
						Field[] subBeanFields = subBeanClazz
								.getDeclaredFields();
						Object subBean = subBeanClazz.newInstance();
						boolean subBeanisAdd = true;
						for (Field subBeanField : subBeanFields) {
							String subBeanFieldName = subBeanField.getName();
							try {
								subBeanPropertyValue = bean
										.get(subBeanFieldName.toUpperCase());
							} catch (Exception e) {
								subBeanPropertyValue = null;
							}

							if (subBeanPropertyValue != null) {
								Method subBeanMethod = subBeanClazz
										.getMethod(
												"set"
														+ subBeanFieldName
																.substring(0, 1)
																.toUpperCase()
														+ subBeanFieldName
																.substring(1),
												subBeanField.getType());
								subBeanMethod.invoke(subBean,
										subBeanPropertyValue);
							} else {
								if (subPrimaryKey.toUpperCase().equals(
										subBeanFieldName.toUpperCase())) {
									subBeanisAdd = false;
								}
							}

						}

						Method listGetMethod = clazz.getMethod("get"
								+ fieldName.substring(0, 1).toUpperCase()
								+ fieldName.substring(1));
						Object listObj = listGetMethod.invoke(obj);
						List<?> list = null;
						if (listObj != null && listObj instanceof List<?>) {
							Method getSubPrimaryKeyMethod = subBeanClazz
									.getMethod("get"
											+ subPrimaryKey.substring(0, 1)
													.toUpperCase()
											+ subPrimaryKey.substring(1));

							list = (List<?>) listObj;
							for (Object obj1 : list) {
								Object subPrimaryKeyValue1 = getSubPrimaryKeyMethod
										.invoke(obj1);
								Object subPrimaryKeyValue2 = getSubPrimaryKeyMethod
										.invoke(subBean);
								if (subPrimaryKeyValue1 != null
										&& subPrimaryKeyValue1
												.equals(subPrimaryKeyValue2)) {
									subBeanisAdd = false;
									break;
								}
							}
							if (subBeanisAdd) {
								Method listAddMethod = listObj.getClass()
										.getMethod("add", Object.class);

								listAddMethod.invoke(listObj, subBean);
							}

						}

					}

					OneToOne oneToOne = field.getAnnotation(OneToOne.class);
					if (oneToOne != null) {
						String subBeanClazzName = oneToOne.bean();
						Class<?> subBeanClazz = Class.forName(subBeanClazzName);
						Field[] subBeanFields = subBeanClazz
								.getDeclaredFields();
						Object subBean = subBeanClazz.newInstance();
						boolean isSubBeanNotNull = false;
						for (Field subBeanField : subBeanFields) {
							String subBeanFieldName = subBeanField.getName();
							try {
								subBeanPropertyValue = bean
										.get(subBeanFieldName.toUpperCase());
								if (subBeanPropertyValue != null) {
									isSubBeanNotNull = true;
								}
							} catch (Exception e) {
								subBeanPropertyValue = null;
							}
							if (subBeanPropertyValue != null) {
								Method subBeanMethod = subBeanClazz
										.getMethod(
												"set"
														+ subBeanFieldName
																.substring(0, 1)
																.toUpperCase()
														+ subBeanFieldName
																.substring(1),
												subBeanField.getType());
								subBeanMethod.invoke(subBean,
										subBeanPropertyValue);
							}

						}
						if (isSubBeanNotNull) {
							Method subBeanSetMethod = clazz.getMethod("set"
									+ fieldName.substring(0, 1).toUpperCase()
									+ fieldName.substring(1), subBeanClazz);
							subBeanSetMethod.invoke(obj, subBean);
						}

					}

				}

				if (isAdd) {
					objects.add(obj);
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				try {
					rs.close();
					rs = null;
				} catch (SQLException e) {
					throw e;
				}

			}

			if (ps != null) {
				try {
					ps.close();
					ps = null;
				} catch (SQLException e) {
					throw e;
				}
			}
		}

		return objects;
	}

	public static void main(String[] args) throws Exception {

		Connection conn = SQLManager.getConnection();
		String result = SQLManager.tableColumnTool("f_dailywork_count", conn);

		System.out.println(result);
	}

}
