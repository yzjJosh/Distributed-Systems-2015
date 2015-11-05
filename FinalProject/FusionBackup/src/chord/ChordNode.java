package chord;

/**
 * Provides all methods necessary for a user application. This includes methods
 * for changing connectivity to the network (create, join, leave) as well as for
 * working with content (insert, retrieve, remove).
 * 
 * @author 	Yu Sun
 */
public class ChordNode {
	/**
	 * The node's finger table. 
	 */
	protected FingerTable fingerTable;
	/**
	 * A reference to this node's successor. 
	 */
	protected ChordNode successor;
	/**
	 * A reference to this node's predecessor. 
	 */
	protected ChordNode predecessor;
	
	
}
