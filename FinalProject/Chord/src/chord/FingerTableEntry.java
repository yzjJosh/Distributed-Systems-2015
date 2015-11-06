package chord;
/**
 * The node's finger table entry. 
 *  * @author 	Yu Sun
 */
class FingerTableEntry {
	protected int start;
	protected int[] interval = new int[2];
	protected int node;
	public FingerTableEntry(int start, int interval_0, int interval_1,  int node) {
		this.start = start;
		this.interval[0] = interval_0;
		this.interval[1] = interval_1;
		this.node = node;
	}
}
