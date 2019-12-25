package omega.persistence;

public class TransactionTypeService {

	private final static ThreadLocal<String> typeHolder = new ThreadLocal<String>();

	public static void setTransactionType(String transactionType) {
		typeHolder.set(transactionType);
	}

	public static String getTransactionType() {
		return typeHolder.get();
	}

	public static boolean isSet() {
		return typeHolder.get() != null;
	}

	public static void remove() {
		typeHolder.remove();
	}

}
