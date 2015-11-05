package exceptions;

/**
 * This exception is thrown whenever the backup operation fails
 * @author Josh
 *
 */
public class BackupFailureException extends Exception {
	
	private static final long serialVersionUID = 1L;
	
	public BackupFailureException(){
		super();
	}
	
	public BackupFailureException(String message){
		super(message);
	}

}
