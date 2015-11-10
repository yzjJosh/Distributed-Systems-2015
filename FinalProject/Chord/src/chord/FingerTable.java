package chord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The node's finger table.  There are m entries and each entry corresponds to one interval.
 *  * @author 	Yu Sun
 */

public class FingerTable implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	FingerTableEntry[] fingers;

	public FingerTable(ChordNode node) {
		this.fingers = new FingerTableEntry[32];
		for (int i = 0; i < fingers.length; i++) {
			ChordID start = node.getChordID();
			fingers[i] = new FingerTableEntry(start, node);
		}
	}

	public FingerTableEntry getFinger(int i) {
		return fingers[i];
	}
	
	
//		List <FingerTableEntry> fingers = new ArrayList<FingerTableEntry> ();
//		int size = 0;
//		/**
//		 * Add a new entry into the finger table of node n.
//		 * 
//		 * @param start: The identity of the first node, s, that succeeds n by at least 2^(i-1) on the identifier circle.
//		 * @param interval_0: finger[k].start
//		 * @param interval_ 1: finger[k+1].start
//		 * @param successor: the previous node on the identifier circle
//		 */
//		public void addNewEntry(ChordID start, ChordID interval_0, ChordID interval_1,  ChordNode node) {
//			FingerTableEntry entry = new FingerTableEntry(start, interval_0, interval_1, node);
//			fingers.add(entry);
//			size++;
//		}
//		
//		public FingerTableEntry getFinger(int index) {
//			return fingers.get(index);
//		}
		
		
}
