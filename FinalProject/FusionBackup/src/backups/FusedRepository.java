package backups;

import java.io.Serializable;
import java.util.*;

import org.jblas.DoubleMatrix;
import org.jblas.Solve;

import exceptions.BackupFailureException;
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
	private final AuxiliaryDataStructure<Serializable>[] auxDataStructures;
	private final ArrayList<FusedNode> dataStack;
	
	/**
	 * Initialize this repository.
	 * @param nodeSize The size of each node in this repository
	 * @param volume The maximum number of primaries that can be fused into this repository
	 * @param id the id of this repository
	 */
	@SuppressWarnings("unchecked")
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
		this.auxDataStructures = (AuxiliaryDataStructure<Serializable>[])new AuxiliaryDataStructure[volume];
		this.dataStack = new ArrayList<FusedNode>();
	}
	
	/**
	 * Put a data of a primary into this repository, this method will create a new entry or update existing entry
	 * @param key the key 
	 * @param prev the previous data, null if no previous data
	 * @param cur the current data
	 * @param primaryId the id of the primary
	 */
	public void putData(Serializable key, ArrayList<Double> prev, ArrayList<Double> cur, int primaryId) throws BackupFailureException{
		try{
			if(primaryId >= volume || primaryId < 0)
				throw new IllegalArgumentException("Illegal primaryId "+primaryId);
			AuxiliaryDataStructure<Serializable> aux = getOrCreateAuxiliaryDataStructure(primaryId);
			if(aux.containsKey(key))
				aux.get(key).fusedNode.updateData(covertToDataVector(prev), covertToDataVector(cur), primaryId);
			else{
				FusedNode fnode = null;
				if(dataStack.size() > aux.size())
					fnode = dataStack.get(aux.size());
				else{
					fnode = new FusedNode(nodeSize, fuseVector, dataStack.size());
					dataStack.add(fnode);
				}
				fnode.updateData(covertToDataVector(prev), covertToDataVector(cur), primaryId);
				AuxiliaryNode anode = new AuxiliaryNode(fnode);
				fnode.setAuxiliaryNode(anode, primaryId);
				aux.put(key, anode);
			}
		}catch(Exception e){
			e.printStackTrace();
			throw new BackupFailureException("Operation fails! Caused by "+e);
		}
	}
	
	/**
	 * Remove a data of a primary from this repository.
	 * @param key The key to remove.
	 * @param val The current value associated with key.
	 * @param end The value of end node of this primary.
	 * @param primaryId The id of the primary
	 */
	public void removeData(Serializable key, ArrayList<Double> val, ArrayList<Double> end, int primaryId) throws BackupFailureException{
		try{
			if(primaryId >= volume || primaryId < 0)
				throw new IllegalArgumentException("Illegal primaryId "+primaryId);
			AuxiliaryDataStructure<Serializable> aux = getOrCreateAuxiliaryDataStructure(primaryId);
			if(!aux.containsKey(key))
				throw new BackupFailureException("Key does not exist! Operation cannot be completed!");
			FusedNode endNode = dataStack.get(aux.size()-1);
			AuxiliaryNode anode = aux.remove(key);
			assert(anode != null);
			anode.fusedNode.updateData(covertToDataVector(val), covertToDataVector(end), primaryId);
			endNode.updateData(covertToDataVector(end), covertToDataVector(null), primaryId);
			endNode.getAuxiliaryNode(primaryId).fusedNode = anode.fusedNode;
			anode.fusedNode.setAuxiliaryNode(endNode.getAuxiliaryNode(primaryId), primaryId);
			endNode.setAuxiliaryNode(null, primaryId);
			if(endNode.fusedNodeNumber() == 0){
				assert(dataStack.get(dataStack.size()-1) == endNode);
				dataStack.remove(dataStack.size()-1);
			}
		}catch(Exception e){
			e.printStackTrace();
			throw new BackupFailureException("Operation fails! Caused by "+e);
		}
	}
	
	
	
	private AuxiliaryDataStructure<Serializable> getOrCreateAuxiliaryDataStructure(int primaryId){
		AuxiliaryDataStructure<Serializable> ret = auxDataStructures[primaryId];
		if(ret == null){
			ret = new AuxiliaryHashMap<Serializable>();
			auxDataStructures[primaryId] = ret;
		}
		return ret;
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
	
	private ArrayList<Double> convertToArrayList(DoubleMatrix m){
		assert(m != null);
		assert(m.length == nodeSize);
		assert(m.isVector());
		ArrayList<Double> ret = new ArrayList<Double>();
		for(int i=0; i<m.length; i++)
			ret.add(m.get(i));
		return ret;
	}
	
	public LinkedList<DataEntry<Serializable, ArrayList<Double>>> getData(int primaryId) throws RecoverFailureException{
		try{
			if(primaryId >= volume || primaryId < 0)
				throw new IllegalArgumentException("Illegal primary id "+primaryId);
			if(auxDataStructures[primaryId] == null)
				throw new RecoverFailureException("Data of primary "+primaryId+" is not in this repository!");
			ArrayList<DoubleMatrix> matrixes = getEncodedMatrixes();
			assert(matrixes != null && matrixes.size() == dataStack.size());
			DoubleMatrix recoverVector = getRecoverVector(primaryId);
			assert(recoverVector != null && recoverVector.length == volume);
			LinkedList<DataEntry<Serializable, ArrayList<Double>>> ret = new LinkedList<DataEntry<Serializable, ArrayList<Double>>>();
			for(DataEntry<Serializable, AuxiliaryNode> entry : auxDataStructures[primaryId].entries()){
				DoubleMatrix decodedData = matrixes.get(entry.value.fusedNode.id).mmul(recoverVector);
				ret.add(new DataEntry<Serializable, ArrayList<Double>>(entry.key, convertToArrayList(decodedData)));
			}
			return ret;
		}catch(Exception e){
			e.printStackTrace();
			throw new RecoverFailureException("Unable to recover data, due to "+e);
		}
	}
	
	private ArrayList<DoubleMatrix> getEncodedMatrixes(){
		ArrayList<DoubleMatrix> ret= new ArrayList<DoubleMatrix>();
		DoubleMatrix mat = new DoubleMatrix(nodeSize, volume);
		mat.putColumn(0, new DoubleMatrix(new double[]{1.0, 5.5}));
		mat.putColumn(volume-1, dataStack.get(0).getFusedData());
		ret.add(mat);
		mat = new DoubleMatrix(nodeSize, volume);
		mat.putColumn(0, new DoubleMatrix(new double[]{2.0, 3.5}));
		mat.putColumn(volume-1, dataStack.get(1).getFusedData());
		ret.add(mat);
		mat = new DoubleMatrix(nodeSize, volume);
		mat.putColumn(volume-1, dataStack.get(2).getFusedData());
		ret.add(mat);
		return ret;
	}
	
	private DoubleMatrix getRecoverVector(int primaryId){
		return Solve.pinv(DoubleMatrix.concatHorizontally(DoubleMatrix.eye(volume).get(new int[]{0,1,2,3}, new int[]{1,2,3}), fuseVector)).getColumn(primaryId);
	}
	
	@Override
	public String toString(){
		StringBuilder str = new StringBuilder("{\n");
		str.append("id: "+id+"\n");
		str.append("node size: "+nodeSize+"\n");
		str.append("volume: "+volume+"\n");
		str.append("Fuse vector: "+fuseVector+"\n");
		str.append("dataStack:\n  [\n");
		for(FusedNode node : dataStack)
			str.append("    "+node+"\n");
		str.append("  ]\n");
		str.append("Auxiliary data structures:\n  [\n");
		for(AuxiliaryDataStructure<Serializable> aux : auxDataStructures)
			str.append("    "+aux+"\n");
		str.append("  ]\n");
		str.append("}\n");
		return str.toString();
	}
	
	public static void main(String[] args){
		FusedRepository repo = new FusedRepository(2, 4, 0);
		try {
			System.out.println(repo);
			repo.putData("Fuck", null, new ArrayList<Double>(Arrays.asList(new Double[]{8.0, 5.5})), 0);
			System.out.println(repo);
			repo.putData("Fuck", null, new ArrayList<Double>(Arrays.asList(new Double[]{0.0, 5.5})), 1);
			System.out.println(repo);
			repo.putData("F", null, new ArrayList<Double>(Arrays.asList(new Double[]{2.0, 3.5})), 1);
			System.out.println(repo);
			repo.putData("G", null, new ArrayList<Double>(Arrays.asList(new Double[]{1.0, 5.5})), 1);
			System.out.println(repo);
			repo.putData("MM", null, new ArrayList<Double>(Arrays.asList(new Double[]{33.5, 5.5})), 0);
			System.out.println(repo);
			repo.putData("Josh", null, new ArrayList<Double>(Arrays.asList(new Double[]{45.9, 88.2})), 0);
			System.out.println(repo);
			repo.removeData("Fuck", new ArrayList<Double>(Arrays.asList(new Double[]{0.0, 5.5})), new ArrayList<Double>(Arrays.asList(new Double[]{1.0, 5.5})), 1);
			System.out.println(repo);
			System.out.println(repo.getData(0));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/*DoubleMatrix B = DoubleMatrix.concatHorizontally(DoubleMatrix.eye(4), repo.fuseVector);
		DoubleMatrix D = DoubleMatrix.linspace(0, 3, 4);
		DoubleMatrix P = D.transpose().mmul(B);
		DoubleMatrix _P = P.get(0, new int[]{0,1,3,4});
		DoubleMatrix _B = B.get(new int[]{0,1,2,3}, new int[]{0,1,3,4});
		System.out.println(_P.mmul(Solve.pinv(_B)));*/
	}
}
