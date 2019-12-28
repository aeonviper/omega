package omega.service;

import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import omega.core.BeanUtility;
import omega.core.Core;

public class Builder<T> {

	public T build(ResultData rd) throws SQLException {
		return null;
	}

	public T build(ResultSet rs) throws SQLException {
		return build(new ResultData(rs));
	}

	public static Builder<Object[]> build(Map<Class, Class> classMap, Map<String, Class> columnTypeMap, Specification[] specificationArray) {
		return new Builder<Object[]>() {

			private Integer[] entityArray = null;
			private String[] entityFieldNameArray = null;

			private void setup(ResultSet rs) throws SQLException {
				ResultSetMetaData metaData = rs.getMetaData();
				int columnCount = metaData.getColumnCount();
				entityArray = new Integer[columnCount];
				entityFieldNameArray = new String[columnCount];
				for (int i = 0; i < specificationArray.length; i++) {
					Specification specification = specificationArray[i];
					String qualifier = specification.getQualifier();
					for (int column = 1; column <= columnCount; column++) {
						String columnName = metaData.getColumnLabel(column);
						if (columnName.startsWith(qualifier)) {
							entityFieldNameArray[column - 1] = lowerCaseFirstCharacter(columnName.substring(qualifier.length()));
							entityArray[column - 1] = i;
						}
					}
				}
			}

			public Object[] build(ResultSet rs) throws SQLException {
				if (entityArray != null && entityFieldNameArray != null) {
				} else {
					setup(rs);
				}

				List list = new ArrayList<>();
				for (Specification specification : specificationArray) {
					list.add(Core.getInjector().getInstance(specification.getClazz()));
				}

				int columnCount = rs.getMetaData().getColumnCount();
				try {
					for (int column = 1; column <= columnCount; column++) {
						String entityFieldName = entityFieldNameArray[column - 1];
						if (entityFieldName != null && entityArray[column - 1] != null) {
							Object entity = list.get(entityArray[column - 1]);
							if (columnTypeMap != null && !columnTypeMap.isEmpty() && columnTypeMap.containsKey(entityFieldName)) {
								BeanUtility.instance().setProperty(entity, entityFieldName, rs.getObject(column, columnTypeMap.get(entityFieldName)));
							} else {
								Object value = rs.getObject(column);
								if (classMap != null && !classMap.isEmpty() && value != null) {
									if (classMap.containsKey(value.getClass())) {
										value = rs.getObject(column, classMap.get(value.getClass()));
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
				return list.toArray();
			}
		};
	}

	protected static String lowerCaseFirstCharacter(String text) {
		if (text != null && text.length() >= 1) {
			return text.substring(0, 1).toLowerCase() + text.substring(1);
		}
		return text;
	}

}
