package chord;
/**
 * The node's finger table entry. 
 *  * @author 	Yu Sun
 */
class FingerTableEntry {
	protected ChordNode start;
	protected ChordNode[] interval = new ChordNode[2];
	protected ChordNode node;
	public FingerTableEntry(ChordNode start, ChordNode interval_0, ChordNode interval_1,  ChordNode node) {
		this.start = start;
		this.interval[0] = interval_0;
		this.interval[1] = interval_1;
		this.node = node;
	}
}
