package omega.service;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import common.BeanUtility;
import common.Core;
import omega.annotation.Transactional;
import omega.persistence.PersistenceService;

public class GenericService {

	@Inject
	PersistenceService persistenceService;

	protected Preparer defaultPreparer = new Preparer();

	@Inject(optional = true)
	public void setDefaultPreparer(@Named("DefaultPreparer") Preparer defaultPreparer) {
		this.defaultPreparer = defaultPreparer;
	}

	protected Map<Class, Class> defaultColumnClassMap = null;

	@Inject(optional = true)
	public void setDefaultColumnClassMap(@Named("DefaultColumnClassMap") Map<Class, Class> defaultColumnClassMap) {
		this.defaultColumnClassMap = defaultColumnClassMap;
	}

	protected Map<String, Class> defaultColumnTypeMap = null;

	@Inject(optional = true)
	public void setDefaultColumnTypeMap(@Named("DefaultColumnTypeMap") Map<String, Class> defaultColumnTypeMap) {
		this.defaultColumnTypeMap = defaultColumnTypeMap;
	}

	public static <T> String join(T[] array, String delimiter, String open, String close) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (T t : array) {
			if (!first) {
				sb.append(delimiter);
			} else {
				first = false;
			}
			if (open != null) {
				sb.append(open);
			}
			if (t != null) {
				sb.append(t);
			}
			if (close != null) {
				sb.append(close);
			}
		}
		return sb.toString();
	}

	public static <T> String join(T[] array, String delimiter) {
		return join(array, delimiter, null, null);
	}

	public static <T> String join(List<T> list, String delimiter, String open, String close) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (T t : list) {
			if (!first) {
				sb.append(delimiter);
			} else {
				first = false;
			}
			if (open != null) {
				sb.append(open);
			}
			if (t != null) {
				sb.append(t);
			}
			if (close != null) {
				sb.append(close);
			}
		}
		return sb.toString();
	}

	public static <T> String join(List<T> list, String delimiter) {
		return join(list, delimiter, null, null);
	}

	public static String repeat(String element, int multiplier, String delimiter) {
		List<String> list = new ArrayList<>();
		for (int i = 0; i < multiplier; i++) {
			list.add(element);
		}
		return join(list, ",");
	}

	// low level

	@Transactional
	public int write(Preparer preparer, String sql, Object... array) {
		int result = 0;
		Connection connection = persistenceService.get().getConnection();
		PreparedStatement stmt = null;
		try {
			stmt = connection.prepareStatement(sql);
			preparer.prepare(stmt, array);
			result = stmt.executeUpdate();
			stmt.close();
			stmt = null;
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return result;
	}

	public int write(String sql, Object... array) {
		return write(this.defaultPreparer, sql, array);
	}

	@Transactional
	public <T> List<T> read(Preparer preparer, boolean asList, Builder<T> builder, String sql, Object... array) {
		List<T> resultList = new ArrayList<>();
		Connection connection = persistenceService.get().getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.prepareStatement(sql);
			preparer.prepare(stmt, array);
			rs = stmt.executeQuery();
			if (rs != null) {
				while (rs.next()) {
					T entity = builder.build(rs);
					resultList.add(entity);
					if (!asList) {
						break;
					}
				}
				rs.close();
				rs = null;
			}
			stmt.close();
			stmt = null;
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return resultList;
	}

	public <T> List<T> read(boolean asList, Builder<T> builder, String sql, Object... array) {
		return read(this.defaultPreparer, asList, builder, sql, array);
	}

	@Transactional
	public <T> void batch(Preparer preparer, int batchSize, String sql, Decorator<T> toDecorator, BatchPreparer<T> batchPreparer, List<T> entityList) {
		int[] result = null;
		Connection connection = persistenceService.get().getConnection();
		PreparedStatement stmt = null;
		int count = 0;
		try {
			stmt = connection.prepareStatement(sql);
			for (T entity : entityList) {
				if (toDecorator != null) {
					toDecorator.decorate(entity);
				}
				stmt.clearParameters();
				preparer.prepare(stmt, batchPreparer.toFieldArray(entity));
				stmt.addBatch();

				if (++count % batchSize == 0) {
					result = stmt.executeBatch();
					if (!checkBatch(result)) {
						throw new RuntimeException("Batch insert exception");
					}
				}
			}

			result = stmt.executeBatch();
			if (!checkBatch(result)) {
				throw new RuntimeException("Batch insert exception");
			}

			stmt.close();
			stmt = null;
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public <T> void batch(String sql, Decorator<T> toDecorator, BatchPreparer<T> batchPreparer, List<T> entityList) {
		batch(this.defaultPreparer, 1000, sql, toDecorator, batchPreparer, entityList);
	}

	public <T> void batch(int batchSize, String sql, Decorator<T> toDecorator, BatchPreparer<T> batchPreparer, List<T> entityList) {
		batch(this.defaultPreparer, batchSize, sql, toDecorator, batchPreparer, entityList);
	}

	public static boolean checkBatch(int[] result) {
		for (int r : result) {
			if (r == 0) {
				return false;
			}
		}
		return true;
	}

	// medium level

	@Transactional
	public <T> int insert(String tableName, T entity, String... array) {
		String sql = "insert into " + tableName + " (" + join(array, ",") + ") values (" + repeat("?", array.length, ",") + ")";
		List parameterList = new ArrayList<>();
		try {
			for (String entry : array) {
				parameterList.add(BeanUtility.instance().getPropertyUtils().getProperty(entity, entry));
			}
			return write(sql, parameterList.toArray());
		} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	@Transactional
	public <T> int update(String tableName, T entity, String[] qualifierArray, String... array) {
		StringBuilder sqlQualifier = new StringBuilder();
		boolean first = true;
		for (String qualifier : qualifierArray) {
			if (first) {
				first = false;
			} else {
				sqlQualifier.append(" and ");
			}
			sqlQualifier.append(qualifier + " = ?");
		}
		List<String> setList = new ArrayList<>();
		for (String element : array) {
			setList.add(element + " = ?");
		}
		String sql = "update " + tableName + " set " + join(setList, ",") + " where (" + sqlQualifier.toString() + ")";
		List parameterList = new ArrayList<>();
		try {
			for (String entry : array) {
				parameterList.add(BeanUtility.instance().getPropertyUtils().getProperty(entity, entry));
			}
			for (String qualifier : qualifierArray) {
				parameterList.add(BeanUtility.instance().getPropertyUtils().getProperty(entity, qualifier));
			}
			return write(sql, parameterList.toArray());
		} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public static List<Field> getAllFields(List<Field> fieldList, Class<?> type) {
		fieldList.addAll(Arrays.asList(type.getDeclaredFields()));
		if (type.getSuperclass() != null) {
			getAllFields(fieldList, type.getSuperclass());
		}
		return fieldList;
	}

	@Transactional
	public <T> List<T> read(Preparer preparer, boolean asList, Class<T> clazz, Map<Class, Class> columnClassMap, Map<String, Class> columnTypeMap, String sql, Object... array) {
		List<T> resultList = new ArrayList<>();

		Map<String, String> fieldMap = new HashMap<>();
		for (Field field : getAllFields(new ArrayList<>(), clazz)) {
			fieldMap.put(field.getName().toLowerCase(), field.getName());
		}

		Connection connection = persistenceService.get().getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.prepareStatement(sql);
			preparer.prepare(stmt, array);
			rs = stmt.executeQuery();
			if (rs != null) {
				ResultSetMetaData metaData = rs.getMetaData();
				int columnCount = metaData.getColumnCount();

				String[] columnNameArray = new String[columnCount];
				for (int column = 1; column <= columnCount; column++) {
					columnNameArray[column - 1] = metaData.getColumnLabel(column).toLowerCase();
				}

				while (rs.next()) {
					T entity = Core.getInjector().getInstance(clazz);
					try {
						for (int column = 1; column <= columnCount; column++) {
							String databaseFieldName = columnNameArray[column - 1];
							String entityFieldName = fieldMap.get(databaseFieldName);
							if (entityFieldName != null) {
								if (columnTypeMap != null && !columnTypeMap.isEmpty() && columnTypeMap.containsKey(entityFieldName)) {
									BeanUtility.instance().setProperty(entity, entityFieldName, rs.getObject(column, columnTypeMap.get(entityFieldName)));
								} else {
									Object value = rs.getObject(column);
									if (columnClassMap != null && !columnClassMap.isEmpty() && value != null) {
										if (columnClassMap.containsKey(value.getClass())) {
											value = rs.getObject(column, columnClassMap.get(value.getClass()));
										}
									}
									BeanUtility.instance().setProperty(entity, entityFieldName, value);
								}
							}
						}
					} catch (IllegalAccessException | InvocationTargetException e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
					resultList.add(entity);
					if (!asList) {
						break;
					}
				}
				rs.close();
				rs = null;
			}
			stmt.close();
			stmt = null;
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return resultList;
	}

	public <T> List<T> read(boolean asList, Class<T> clazz, Map<Class, Class> columnClassMap, Map<String, Class> columnTypeMap, String sql, Object... array) {
		return read(this.defaultPreparer, asList, clazz, columnClassMap, columnTypeMap, sql, array);
	}

	// high level

	public <T> List<T> list(Builder<T> builder, String sql, Object... array) {
		return read(true, builder, sql, array);
	}

	public <T> T find(Builder<T> builder, String sql, Object... array) {
		List<T> list = read(false, builder, sql, array);
		if (!list.isEmpty()) {
			return list.get(0);
		}
		return null;
	}

	public <T> T select(Class<T> clazz, String sql, Object... array) {
		Builder<T> builder = new Builder<T>() {
			public T build(ResultSet rs) throws SQLException {
				return rs.getObject(1, clazz);
			}
		};

		return find(builder, sql, array);
	}

	public <T> List<T> list(Class<T> clazz, Map<Class, Class> columnClassMap, Map<String, Class> columnTypeMap, String sql, Object... array) {
		return read(true, clazz, columnClassMap, columnTypeMap, sql, array);
	}

	public <T> List<T> list(Class<T> clazz, String sql, Object... array) {
		return list(clazz, defaultColumnClassMap, defaultColumnTypeMap, sql, array);
	}

	public <T> T find(Class<T> clazz, Map<Class, Class> columnClassMap, Map<String, Class> columnTypeMap, String sql, Object... array) {
		List<T> list = read(false, clazz, columnClassMap, columnTypeMap, sql, array);
		if (!list.isEmpty()) {
			return list.get(0);
		}
		return null;
	}

	public <T> T find(Class<T> clazz, String sql, Object... array) {
		return find(clazz, defaultColumnClassMap, defaultColumnTypeMap, sql, array);
	}

}
