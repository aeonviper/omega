package omega.persistence;

public interface TransactionTypeAllower {
	
	public boolean isAllowed(String current, String enter);

}
