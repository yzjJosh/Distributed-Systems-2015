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
			long start_id = (long) ((node.getChordID().getID() + 1 + (long) Math.pow(2, i-1)) % (1L<<32)); 
			ChordID start = new ChordID(start_id);
			fingers[i] = new FingerTableEntry(start, node);
		}
		
		for (int i = 0; i < fingers.length; i++) {
			if (i == fingers.length - 1) {
				ChordID interval_1 = fingers[i].start ;
				ChordID interval_2 = new ChordID(fingers[0].start.getID() - 1);
				fingers[i].setInterval(interval_1, interval_2);
			} else {
				ChordID interval_1 = fingers[i].start;
				ChordID interval_2 = fingers[i+1].start;
				fingers[i].setInterval(interval_1, interval_2);
			}
			
		}
	}

	public FingerTableEntry getFinger(int i) {
		return fingers[i];
	}
	
	public void print() {
		System.out.println("********---------------------------------------------------------------------***********");
		for (int i = 0; i < fingers.length; i++) {
			System.out.println("start: " + fingers[i].start.getID() + " | interval: [" + fingers[i].interval[0].getID() + " , " + fingers[i].interval[1].getID() + ") | successor: " + fingers[i].node.getChordID().getID());
		}
		System.out.println("********---------------------------------------------------------------------***********");
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
