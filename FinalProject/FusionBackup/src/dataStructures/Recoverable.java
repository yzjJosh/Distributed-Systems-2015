package dataStructures;

import exceptions.RecoverFailureException;

/**
 * A Recoverable object can be backed up and recovered.
 * @author Josh
 *
 */
public interface Recoverable {
	
	/**
	 * Recover data from backup
	 * @throws RecoverFailureException if the recovery fails
	 */
	public void recover() throws RecoverFailureException;
	
}
