package exceptions;

/**
 * This exception is thrown whenever an operation fails
 * @author Josh
 *
 */
public class OperationFailsException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public OperationFailsException(){
		super();
	}
	
	public OperationFailsException(String message){
		super(message);
	}

}
