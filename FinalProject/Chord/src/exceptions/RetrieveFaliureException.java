package exceptions;

/**
 * This exception is thrown when retrieval failed due to a communication failure or some other reasons. 
 *
 * @author Yu Sun
 *
 */
public class RetrieveFaliureException extends Exception {
	
	private static final long serialVersionUID = 1L;
	
	public RetrieveFaliureException(){
		super();
	}
	
	public RetrieveFaliureException(String message){
		super(message);
	}

}
