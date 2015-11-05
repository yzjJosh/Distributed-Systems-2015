package exceptions;

/**
 * This exception is thrown whenever there is an unexcepted remote internal error
 * @author Josh
 *
 */
public class RemoteInternalErrorException extends Exception {
	
	private static final long serialVersionUID = 1L;

	public RemoteInternalErrorException(){
		super();
	}
	
	public RemoteInternalErrorException(String message){
		super(message);
	}
	
}
