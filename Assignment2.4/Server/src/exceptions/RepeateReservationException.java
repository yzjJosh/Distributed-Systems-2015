package exceptions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;





/**
 * This exception is thrown when a client reserve seates repeatly.
 */
public class RepeateReservationException extends Exception {

	
	private static final long serialVersionUID = 1L;
    public HashSet<Integer> reservedSeats = null;
	/**
	 * Create an empty exception.
	 */
	public RepeateReservationException(){
		
		super();
		
	}
	
	/**
	 * Create an exception with a message.
	 * @param set The message.
	 */
	public RepeateReservationException(Set<Integer> set){
		super();
		reservedSeats = (HashSet) set;
	}

}
