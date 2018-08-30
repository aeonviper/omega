package omega.persistence;

import static com.google.inject.matcher.Matchers.annotatedWith;
import static com.google.inject.matcher.Matchers.any;

import org.aopalliance.intercept.MethodInterceptor;

import com.google.inject.AbstractModule;
import com.zaxxer.hikari.HikariDataSource;

import omega.annotation.Transactional;

public class PersistenceModule extends AbstractModule {

	public PersistenceModule() {
	}

	@Override
	protected final void configure() {

		HikariDataSource ds = new HikariDataSource();
		ds.setDriverClassName("org.postgresql.Driver");
		ds.setJdbcUrl("jdbc:postgresql://localhost:5432/database");
		ds.setUsername("username");
		ds.setPassword("password");

		bind(PersistenceService.class).toInstance(new PersistenceService(ds));
		MethodInterceptor transactionInterceptor = new TransactionInterceptor();
		requestInjection(transactionInterceptor);

		bindInterceptor(any(), annotatedWith(Transactional.class), transactionInterceptor);

	}

}
