package omega.service;

import com.google.inject.Singleton;
import omega.annotation.Transactional;

@Singleton
public class TransactionService {

	@Transactional
	public void execute(TransactionContext transactionContext) {
		transactionContext.execute();
	}

	@Transactional
	public void execute(TransactionContext transactionContext, Object... array) {
		transactionContext.execute(array);
	}

	@Transactional
	public <T> T action(TransactionContext<T> transactionContext) {
		return transactionContext.action();
	}

	@Transactional
	public <T> T action(TransactionContext<T> transactionContext, Object... array) {
		return transactionContext.action(array);
	}

}
