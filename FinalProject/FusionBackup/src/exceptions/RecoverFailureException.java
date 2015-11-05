package exceptions;

/**
 * This exception is thrown whenever recovery fails.
 * @author Josh
 *
 */
public class RecoverFailureException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public RecoverFailureException(){
		super();
	}
	
	public RecoverFailureException(String message){
		super(message);
	}

}
