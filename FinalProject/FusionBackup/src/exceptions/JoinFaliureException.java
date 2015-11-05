package exceptions;

/**
 * This exception is thrown whenever a new node join the chord ring and fails.
 * @author Yu Sun
 *
 */
public class JoinFaliureException extends Exception {
	
	private static final long serialVersionUID = 1L;
	
	public JoinFaliureException(){
		super();
	}
	
	public JoinFaliureException(String message){
		super(message);
	}

}

