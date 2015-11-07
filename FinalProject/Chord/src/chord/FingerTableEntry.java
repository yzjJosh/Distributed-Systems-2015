package chord;
/**
 * The node's finger table entry. 
 *  * @author 	Yu Sun
 */
class FingerTableEntry {
	protected ChordID start;
	protected ChordID[] interval = new ChordID[2];
	protected ChordNode node;
	public FingerTableEntry(ChordID start, ChordID interval_0, ChordID interval_1,  ChordNode node) {
		this.start = start;
		this.interval[0] = interval_0;
		this.interval[1] = interval_1;
		this.node = node;
	}
	public ChordNode getNode() {
		return node;
	}
	public ChordID getStart() {
		return start;
	}

	public void setStart(ChordID start) {
		this.start = start;
	}


	public void setNode(ChordNode node) {
		this.node = node;
	}
}
