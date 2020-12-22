package omega.persistence;

import java.sql.Connection;

import javax.sql.DataSource;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import com.google.inject.Inject;

import omega.annotation.TransactionIsolation;
import omega.annotation.Transactional;

public class TransactionInterceptor implements MethodInterceptor {

	@Inject
	PersistenceService persistenceService;

	public Object invoke(MethodInvocation methodInvocation) throws Throwable {
		Transactional transactional = methodInvocation.getMethod().getAnnotation(Transactional.class);
		if (transactional != null) {

			DataSource dataSource = null;
			String transactionType = null;

			if (persistenceService.isMultiple()) {
				if (persistenceService.get() == null) {
					transactionType = TransactionTypeService.isSet() ? TransactionTypeService.getTransactionType() : transactional.type();
					dataSource = persistenceService.getDataSource(transactionType);
				} else {
					if (!persistenceService.isAllowed(persistenceService.get().getTransactionType(), transactional.type())) {
						String message = "Transaction type progression not allowed ! Current transaction is " + persistenceService.get().getTransactionType() + " but method " + methodInvocation.getClass().getCanonicalName() + "." + methodInvocation.getMethod().getName() + " has transactional type " + transactional.type();
						throw new RuntimeException(this.getClass().getCanonicalName() + " - " + message);
					}

					// this is an override, but the current transaction (which is set in the beginning of the thread execution) does not match the override
					// this is probably not going to happen, since override can only happen at the beginning not during a thread execution
					// transaction override is only honored by the framework only at the beginning of thread execution
					// e.g.
					// if it wanted read write it should have specified read write in the beginning thus persistenceService would hold a read write data source
					// in order for this to happen it means ThreadLocal of transaction type at beginning of thread execution is one value (read only) and got changed to a different value (read write) in the middle of thread execution, now the framework is re-checking and finding a different value
					if (TransactionTypeService.isSet() && !persistenceService.get().getTransactionType().equals(TransactionTypeService.getTransactionType())) {
						String message = "Transaction type mismatch ! Current Transaction is " + persistenceService.get().getTransactionType() + " but transaction type override is " + TransactionTypeService.getTransactionType();
						throw new RuntimeException(this.getClass().getCanonicalName() + " - " + message);
					}
				}
			} else {
				if (persistenceService.get() == null) {
					dataSource = persistenceService.getDataSource();
					transactionType = persistenceService.defaultName;
				}
			}

			Connection connection = null;
			if (persistenceService.get() == null) {

				connection = dataSource.getConnection();
				connection.setAutoCommit(false);

				TransactionIsolation transactionalIsolation = transactional.isolation();
				if (transactionalIsolation != TransactionIsolation.DEFAULT) {
					switch (transactionalIsolation) {
					case READ_COMMITTED:
						connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
						break;
					case READ_UNCOMMITTED:
						connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
						break;
					case REPEATABLE_READ:
						connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
						break;
					case SERIALIZABLE:
						connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
						break;
					default:
						break;
					}
				}

				persistenceService.set(new PersistenceTransaction(connection, transactionType));

			} else {

				// check requested transaction isolation
				// Connection databaseConnection = persistenceService.get().getConnection();

			}

			Object result = null;

			if (dataSource != null) {

				try {
					result = methodInvocation.proceed();
					connection.commit();
				} catch (Exception e) {
					if (commitWhenException(transactional, e)) {
						connection.commit();
					} else {
						connection.rollback();
					}

					System.err.println(e);
					throw e;
				} finally {
					connection.close();
					persistenceService.remove();
				}

			} else {

				result = methodInvocation.proceed();

			}

			return result;

		} else {
			return methodInvocation.proceed();
		}
	}

	private boolean commitWhenException(Transactional transactional, Exception e) {
		boolean commit = true;

		for (Class<? extends Exception> rollBackOn : transactional.rollbackOn()) {
			if (rollBackOn.isInstance(e)) {
				commit = false;

				for (Class<? extends Exception> exceptOn : transactional.ignore()) {
					if (exceptOn.isInstance(e)) {
						commit = true;
						break;
					}
				}

				break;
			}
		}

		return commit;
	}
}
