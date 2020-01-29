package omega.service;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Preparer {

	public void prepare(PreparedStatement stmt, Object[] array) throws SQLException {
		int i = 1;
		for (Object object : array) {
			if (object == null) {
				stmt.setNull(i++, java.sql.Types.NULL);
			} else if (object instanceof String) {
				stmt.setString(i++, (String) object);
			} else if (object instanceof Long) {
				stmt.setLong(i++, (Long) object);
			} else if (object instanceof Integer) {
				stmt.setInt(i++, (Integer) object);
			} else if (object instanceof Boolean) {
				stmt.setBoolean(i++, (Boolean) object);
			} else if (object instanceof Enum) {
				stmt.setString(i++, ((Enum) object).name());
			} else if (object instanceof java.math.BigDecimal) {
				stmt.setObject(i++, (java.math.BigDecimal) object);
			} else if (object instanceof java.sql.Timestamp) {
				stmt.setTimestamp(i++, (java.sql.Timestamp) object);
			} else if (object instanceof java.sql.Date) {
				stmt.setDate(i++, (java.sql.Date) object);
			} else if (object instanceof java.time.LocalDate) {
				stmt.setObject(i++, object);
			} else if (object instanceof java.time.LocalDateTime) {
				stmt.setObject(i++, object);
			} else if (object instanceof java.util.Date) {
				stmt.setDate(i++, convertDate((java.util.Date) object));
			} else {
				stmt.setString(i++, object.toString());
			}
		}
	}

	public static java.sql.Date convertDate(java.util.Date date) {
		if (date != null) {
			return new java.sql.Date(date.getTime());
		}
		return null;
	}

}
