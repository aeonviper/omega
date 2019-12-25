package omega.persistence;

import java.sql.Connection;

public class PersistenceTransaction {

	private Connection connection;
	private String transactionType;

	public PersistenceTransaction(Connection connection, String transactionType) {
		this.connection = connection;
		this.transactionType = transactionType;
	}

	public Connection getConnection() {
		return connection;
	}

	public String getTransactionType() {
		return transactionType;
	}

}
