package backups;

import java.util.Arrays;

import org.jblas.DoubleMatrix;

/**
 * A node which contains fused data
 * @author Josh
 *
 */
public class FusedNode{
	
	public final int id;
	private final DoubleMatrix data;
	private final AuxiliaryNode[] auxs;
	private final DoubleMatrix fuseVector;
	private int fusedDataCount = 0;
	
	public FusedNode(int size, DoubleMatrix fuseVector, int id){
		if(size < 1)
			throw new IllegalArgumentException("Illegal size "+size);
		assert(fuseVector != null && fuseVector.length > 0);
		data = DoubleMatrix.zeros(size);
		auxs = new AuxiliaryNode[fuseVector.length];
		this.fuseVector = fuseVector;
		this.id = id;
	}
	
	public void updateData(DoubleMatrix prev, DoubleMatrix cur, int primaryId){
		if(primaryId >= fuseVector.length || primaryId < 0)
			throw new IndexOutOfBoundsException("Illegal primary id "+primaryId);
		if(prev == null || cur == null)
			throw new NullPointerException("Vector should be specified");
		if(prev.length != data.length || cur.length != data.length)
			throw new IllegalArgumentException("Vector dimension does not match! Got "+prev.length+" and "+cur.length+" ,which should be "+data.length+" and "+data.length);
		data.addi(cur.sub(prev).mul(fuseVector.get(primaryId)));
	}
	
	public void setAuxiliaryNode(AuxiliaryNode node, int primaryId){
		if(primaryId >= fuseVector.length || primaryId < 0)
			throw new IndexOutOfBoundsException("Illegal primary id "+primaryId);
		if(auxs[primaryId] == null && node != null)
			fusedDataCount++;
		else if(auxs[primaryId] != null && node == null)
			fusedDataCount--;
		auxs[primaryId] = node;
	}
	
	public AuxiliaryNode getAuxiliaryNode(int primaryId){
		if(primaryId >= fuseVector.length || primaryId < 0)
			throw new IndexOutOfBoundsException("Illegal primary id "+primaryId);
		return auxs[primaryId];
	}
	
	public int fusedNodeNumber(){
		return fusedDataCount;
	}
	
	public DoubleMatrix getFusedData(){
		return data;
	}
	
	@Override
	public String toString(){
		return "{id="+id+", data="+data.toString()+", auxs="+Arrays.toString(auxs)+"}";
	}
	
}
