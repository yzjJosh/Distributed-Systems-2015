package message;


/**
 * Message filter is an interface that is used to filt messages in specific rules.
 *
 */
public interface MessageFilter {
	
	/**
	 * Filt a message in some rules
	 * @param msg The message
	 * @return	If this message match the rule or not.
	 */
	public boolean filt(Message msg);
}
