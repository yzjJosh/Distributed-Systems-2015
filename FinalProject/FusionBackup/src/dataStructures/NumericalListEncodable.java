package dataStructures;

import java.util.ArrayList;

/**
 * A NumericalListEncodable object can be encoded into a numerical (specifically, double number) list.
 * @author Josh
 *
 */
public interface NumericalListEncodable {
	
	/**
	 * Encode this object into a numerical list.
	 * @return The encoded list
	 */
	public ArrayList<Double> encode();

	
}
