package exceptions;

/**
 * TerminateException is used to terminate a thread.
 * @author Josh
 *
 */
public class TerminateException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public TerminateException(){
		super();
	}
	
	public TerminateException(String message){
		super(message);
	}
	
}
