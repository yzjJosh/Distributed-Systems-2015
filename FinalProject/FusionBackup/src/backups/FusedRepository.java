package backups;

/**
 * Repository of fused backup data.
 * @author Josh
 *
 */
public class FusedRepository {
	public final DoubleMatrix data;
	
	/**
	 * Initialize this repository and set the maximum node size of this repository.
	 * @param maxNodeSize The maximum size of each node in this repository
	 */
	public FusedRepository(int maxNodeSize){
		this.maxNodeSize = maxNodeSize;
	}
}
