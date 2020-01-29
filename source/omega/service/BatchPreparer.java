package omega.service;

public interface BatchPreparer<T> {

	public Object[] toFieldArray(T entity);

}
