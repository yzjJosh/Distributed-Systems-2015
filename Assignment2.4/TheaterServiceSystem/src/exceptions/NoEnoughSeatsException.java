package exceptions;

/**
 * This exception is thrown when there is not enough seats.
 */
public class NoEnoughSeatsException extends Exception {
	
	private static final long serialVersionUID = 1L;

	/**
	 * Create an empty exception.
	 */
	public NoEnoughSeatsException(){
		super();
	}
	
	/**
	 * Create an exception with a message.
	 * @param s The message included in exception.
	 */
	public NoEnoughSeatsException(String s){
		super(s);
	}
}
