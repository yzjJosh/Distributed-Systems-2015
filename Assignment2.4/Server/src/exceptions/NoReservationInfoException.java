package exceptions;

/**
 * This exception is thrown when try to find a non-existed researvation.
 */
public class NoReservationInfoException extends Exception {

	private static final long serialVersionUID = 1L;
	
	/**
	 * Create an empty exception.
	 */
	public NoReservationInfoException(){
		super();
	}
	
	/**
	 * Create an exception with a message.
	 * @param s The message.
	 */
	public NoReservationInfoException(String s){
		super(s);
	}

}
