package chord;
/**
 * The node's finger table entry. 
 *  * @author 	Yu Sun
 */
class FingerTableEntry {
	private int start;
	private int[] interval = new int[2];
	private int successor;
	public FingerTableEntry(int start, int interval_0, int interval_1,  int successor) {
		this.start = start;
		this.interval[0] = interval_0;
		this.interval[1] = interval_1;
		this.successor = successor;
	}
}
