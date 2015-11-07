package chord;
/**
 * The identifier of each node.
 * @author 	Yu Sun
 */
public class ChordID implements Comparable {
	private int id = 0;
	public ChordID (String identifier) {
		id = identifier.hashCode(); 
	}
	public int getID() {
		return id;
	}
	
	public void setID(int id) {
		this.id = id;
	}
	
	@Override
	public int compareTo(Object o) {
		ChordID objective = (ChordID) o;
		if(id < objective.id) 
			return -1;
		else if(id > objective.id) 
			return 1;
		else 
			return 0;	
	}
	@Override
	public boolean equals(Object o) {
		if (this.id == ((ChordID)o).id) 
			return true;
		else
			return false;
	}
	public boolean isBetween(ChordID c1, ChordID c2) {
		if (this.compareTo(c1) == 1) {
			if (this.compareTo(c2) == -1)
				return true;	
		}
		return false;
	}
	
	static public void main(String args) {

	}

}
