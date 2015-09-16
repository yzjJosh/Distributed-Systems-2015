package exceptions;





/**
 * This exception is thrown when a client reserve seates repeatly.
 */
public class RepeateReservationException extends Exception {

	
	private static final long serialVersionUID = 1L;

	/**
	 * Create an empty exception.
	 */
	public RepeateReservationException(){
		
		super();
		
	}
	
	/**
	 * Create an exception with a message.
	 * @param s The message.
	 */
	public RepeateReservationException(String s){
		super(s);
	}

}
