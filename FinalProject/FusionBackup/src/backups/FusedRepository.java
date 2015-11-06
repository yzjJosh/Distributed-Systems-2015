package backups;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.jblas.DoubleMatrix;
import org.jblas.Solve;
import org.jblas.ranges.Range;

import exceptions.RecoverFailureException;

/**
 * Repository of fused backup data.
 * @author Josh
 *
 */
public class FusedRepository {
	
	public final int id;
	public final int nodeSize;
	public final int volume;
	private final DoubleMatrix fuseVector;
	private final ArrayList<AuxiliaryDataStructure<Serializable>> auxDataStructures;
	
	/**
	 * Initialize this repository.
	 * @param nodeSize The size of each node in this repository
	 * @param volume The maximum number of primaries that can be fused into this repository
	 * @param id the id of this repository
	 */
	public FusedRepository(int nodeSize, int volume, int id){
		if(nodeSize <= 0)
			throw new IllegalArgumentException("Illegal nodeSize "+nodeSize);
		if(volume <= 0)
			throw new IllegalArgumentException("Illegal volume "+volume);
		if(id < 0 || id >= volume)
			throw new IllegalArgumentException("Illegal id "+id);
		this.id = id;
		this.nodeSize = nodeSize;
		this.volume = volume;
		this.fuseVector = generateFuseVector();
		this.auxDataStructures = new ArrayList<AuxiliaryDataStructure<Serializable>>();
	}
	
	private DoubleMatrix generateFuseVector(){
		int n = volume;
		DoubleMatrix A = new DoubleMatrix(n, n);
		DoubleMatrix vector = DoubleMatrix.ones(n);
		DoubleMatrix mul = DoubleMatrix.linspace(1, n, n);
		for(int i=0; i<n; i++){
			A.putColumn(i, vector);
			vector = vector.mul(mul);
		}
		for(int i=0; i<id; i++)
			vector = vector.mul(mul);
		return Solve.solve(A, vector);
	}
	
	private DoubleMatrix covertToDataVector(ArrayList<Double> data){
		if(data != null && data.size() > nodeSize)
			throw new IllegalArgumentException("Data size "+data.size()+" exceeds node size "+nodeSize);
		DoubleMatrix ret = DoubleMatrix.zeros(nodeSize);
		if(data != null)
			for(int i=0; i<data.size(); i++)
				ret.put(i, data.get(i));
		return ret;
	}
	
	private List<DataEntry<Serializable, DoubleMatrix>> getData(int primaryId) throws RecoverFailureException{
		if(primaryId >= auxDataStructures.size() || primaryId < 0)
			throw new IndexOutOfBoundsException("Illegal primary id "+primaryId);
		
		return null;
	}
	
	public static void main(String[] args){
		
	}
}
