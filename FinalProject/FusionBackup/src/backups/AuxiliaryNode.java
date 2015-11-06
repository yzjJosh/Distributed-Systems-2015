package backups;

import java.io.Serializable;

/**
 * Auxiliary node which represents a node in primary.
 * @author Josh
 *
 */
public class AuxiliaryNode implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public FusedNode fusedNode;
	
	public AuxiliaryNode(FusedNode fusedNode){
		this.fusedNode = fusedNode;
	}
	
}
