package interfaces;

import exceptions.RecoverFailureException;

/**
 * A Backupable object can be backed up.
 * @author Josh
 *
 */
public interface Backupable {
	
	/**
	 * Get the number of active back up nodes
	 * @return the number of active back up nodes
	 */
	public int activeBackupNodes();
	
	/**
	 * Recover data from backup node
	 * @throws RecoverFailureException if the recovery fails
	 */
	public void recover() throws RecoverFailureException;
	
}
