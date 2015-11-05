package interfaces;

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
	
	/**
	 * Decode an object from a numericallist.
	 * @param numericalList The encoded list
	 * @return The decoded object. The returned object and the method-invoking-object should have the same type.
	 * @throws DecodeException When there is an error in decoding
	 */
	public NumericalListEncodable decode(ArrayList<Double> numericalList);
}
