package exceptions;

/**
 * DecodeException is thrown whenever decoding fails
 * @author Josh
 *
 */
public class DecodeException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	
	public DecodeException(){
		super();
	}
	
	public DecodeException(String msg){
		super(msg);
	}

}
