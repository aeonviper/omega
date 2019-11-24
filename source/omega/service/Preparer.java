package omega.service;

public interface Preparer<T> {

	public Object[] toFieldArray(T entity);

}
