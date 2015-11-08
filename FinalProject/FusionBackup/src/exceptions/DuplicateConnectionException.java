package exceptions;

/**
 * This exception is thrown when duplicate connection is detected
 * @author Josh
 *
 */
public class DuplicateConnectionException extends Exception {

	private static final long serialVersionUID = 1L;

	public DuplicateConnectionException(){
		super();
	}
	
	public DuplicateConnectionException(String message){
		super(message);
	}
}
