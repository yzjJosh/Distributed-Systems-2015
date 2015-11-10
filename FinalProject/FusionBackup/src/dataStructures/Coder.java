package dataStructures;

import java.util.ArrayList;

import exceptions.DecodeException;

/**
 * A Coder can encode an object into a numerical (specifically, double number) list, or decode it from the list.
 * @author Josh
 *
 */
public interface Coder<T> {
	
	/**
	 * Encode this object into a numerical list.
	 * @param target the target which needs encoding, which may be null
	 * @return The encoded list, which should not be null
	 */
	public ArrayList<Double> encode(T target);

	/**
	 * Decode an object from a numerical list. You should ensure that decode(encode(target)) = target.
	 * @param source The source to decode, which must not be null
	 * @return the decoded target, which may be null
	 * @throws DecodeException If there is error when decoding
	 */
	public T decode(ArrayList<Double> source) throws DecodeException;
}
