package backups;

import java.util.ArrayList;

import org.jblas.DoubleMatrix;

/**
 * A node which contains fused data
 * @author Josh
 *
 */
public class FusedNode{
	
	private final DoubleMatrix data;
	private final ArrayList<AuxiliaryNode> auxs;
	private final DoubleMatrix fuseVector;
	
	public FusedNode(int size, DoubleMatrix fuseVector){
		if(size < 1)
			throw new IllegalArgumentException("Illegal size "+size);
		data = DoubleMatrix.zeros(size);
		auxs = new ArrayList<AuxiliaryNode>();
		this.fuseVector = fuseVector;
	}
	
	public void updateData(DoubleMatrix prev, DoubleMatrix cur, int primaryId){
		if(primaryId >= auxs.size() || primaryId < 0)
			throw new IndexOutOfBoundsException("Illegal primary id "+primaryId);
		if(prev == null || cur == null)
			throw new NullPointerException("Vector should be specified");
		if(prev.length != data.length || cur.length != data.length)
			throw new IllegalArgumentException("Vector dimension does not match! Got "+prev.length+" and "+cur.length+" ,which should be "+data.length+" and "+data.length);
		data.addi(cur.sub(prev).mul(fuseVector.get(primaryId)));
	}
	
	public DoubleMatrix getDusedData(){
		return data;
	}
	
	@Override
	public String toString(){
		return "{data="+data.toString()+", auxs="+auxs.toString()+"}";
	}
	
}
