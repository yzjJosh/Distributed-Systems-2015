package dataStructures;

import java.util.ArrayList;

/**
 * A NumericalListEncodable object can be encoded into a numerical (specifically, double number) list.
 * @author Josh
 *
 */
public interface NumericalListEncodable {
	
	/**
	 * Encode this object into a numerical list. Note that null pointer will be encoded into {0, 0, ...} in the back up node,
	 * so be careful to encode an object into {0, 0, ...}. (You need either ensure no null pointer will be backed up, or 
	 * never encode an object into {0, 0, ...}
	 * @return The encoded list
	 */
	public ArrayList<Double> encode();

}
