package communication;

/**
 * Message filter is used to filter some specific kind of message
 * @author Josh
 *
 */
public interface MessageFilter {
	
	/**
	 * Check if a message conforms a rule
	 * @param msg the message
	 * @return if it conforms the rule or not
	 */
	public boolean filter(Message msg);
	
}
