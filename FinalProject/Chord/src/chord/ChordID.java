package chord;

import java.io.Serializable;

/**
 * The identifier of each node.
 * @author 	Yu Sun
 */
public class ChordID implements Comparable, Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long id = 0;
	public ChordID (String identifier) {
		id = Math.abs(identifier.hashCode()); 
	}
	public ChordID (long identifier) {
		id = identifier; 
	}
	public long getID() {
		return id;
	}
	
	public void setID(long id) {
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
		if (c1.compareTo(c2) == -1) {
			if (this.compareTo(c1) == 1 && this.compareTo(c2) == -1) {
				return true;	
			}
		} else if (c1.compareTo(c2) == 1) {
			if (this.compareTo(c1) == 1 || this.compareTo(c2) == -1){
				return true;
			}
		} else if (c1.compareTo(c2) == 0) {
			return this.compareTo(c1) != 0;
		}
		
		return false;
	}
	


}
