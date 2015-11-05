package exceptions;

/**
 * This exception is thrown whenever the creation of a new node fails.
 * @author Yu Sun
 *
 */
public class NodeFaliureException extends Exception {
	
	private static final long serialVersionUID = 1L;
	
	public NodeFaliureException(){
		super();
	}
	
	public NodeFaliureException(String message){
		super(message);
	}

}
