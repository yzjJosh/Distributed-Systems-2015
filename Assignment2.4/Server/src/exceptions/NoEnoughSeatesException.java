package exceptions;

/**
 * This exception is thrown when there is not enough seates.
 */
public class NoEnoughSeatesException extends Exception {
	
	private static final long serialVersionUID = 1L;

	/**
	 * Create an empty exception.
	 */
	public NoEnoughSeatesException(){
		super();
	}
	
	/**
	 * Create an exception with a message.
	 * @param s The message included in exception.
	 */
	public NoEnoughSeatesException(String s){
		super(s);
	}
}
