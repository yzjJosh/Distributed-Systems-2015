package backups;

import java.io.Serializable;

/**
 * Auxiliary node which represents a node in primary.
 * @author Josh
 *
 */
public class AuxiliaryNode implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public final Serializable key;
	public FusedNode fusedNode;
	
	public AuxiliaryNode(Serializable key, FusedNode fusedNode){
		this.key = key;
		this.fusedNode = fusedNode;
	}
	
	@Override
	public String toString(){
		return key+"= fusedNode["+fusedNode.id+"]";
	}
	
}
