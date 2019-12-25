package omega.persistence;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;
import com.google.inject.Singleton;

@Singleton
public class PersistenceService {

	public static final String defaultName = "";
	private final ThreadLocal<PersistenceTransaction> persistenceTransactionRepository = new ThreadLocal<PersistenceTransaction>();
	protected final Map<String, DataSource> dataSourceMap = new HashMap<>();
	protected TransactionTypeAllower allower = null;

	public PersistenceTransaction get() {
		return persistenceTransactionRepository.get();
	}

	public void set(PersistenceTransaction persistenceTransaction) {
		persistenceTransactionRepository.set(persistenceTransaction);
	}

	public void remove() {
		persistenceTransactionRepository.remove();
	}

	public PersistenceService() {

	}

	public PersistenceService(DataSource dataSource) {
		dataSourceMap.put(defaultName, dataSource);
	}

	public void registerDataSource(String name, DataSource dataSource) {
		dataSourceMap.put(name, dataSource);
	}

	public void unregisterDataSource(String name) {
		dataSourceMap.remove(name);
	}

	public void setAllower(TransactionTypeAllower allower) {
		this.allower = allower;
	}

	public void unsetAllower() {
		this.allower = null;
	}

	public DataSource getDataSource() {
		return dataSourceMap.get(defaultName);
	}

	public DataSource getDataSource(String name) {
		return dataSourceMap.get(name);
	}

	public boolean isMultiple() {
		return dataSourceMap.size() > 1;
	}

	public boolean isAllowed(String current, String enter) {
		return allower == null || allower.isAllowed(current, enter);
	}

}
