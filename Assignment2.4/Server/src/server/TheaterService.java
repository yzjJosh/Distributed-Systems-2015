package server;

import java.io.Serializable;
import java.util.*;

import exceptions.*;

/**
 * TheaterService deals with core functions of seate reservation service.
 *
 */
public class TheaterService implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private HashMap<String, Set<Integer>> reservedSeats; //Name to reserved seates.
	Stack <Integer> emptySeats = new Stack <Integer>();
	
	public TheaterService(int numOfSeats) {
		for(int i = 1; i <= numOfSeats; i++){
			emptySeats.push(i);
		}
		reservedSeats  = new HashMap<String, Set<Integer>>();
	}
	/**
	 * Reserve certain number of seats for client.
	 * @param name The name of client.
	 * @param count The number of seats to reserve.
	 * @return The reserved seats number.
	 * @throws NoEnoughSeatesException When there is no enough seats.
	 * @throws RepeateReservationException When the client has already reserved seats.
	 */
	public Set<Integer> reserve(String name, int count) throws NoEnoughSeatsException, RepeateReservationException{
		
		Set<Integer> set = new HashSet <Integer> ();
		//If the client has already reserved seats, then throws an exception
		if(reservedSeats.containsKey(name)) { 
			throw new RepeateReservationException(reservedSeats.get(name));
		}
		//If there is no enough seats, then throws an exception
		if(count > emptySeats.size()){
			System.out.println(count + "empty seats left: " + emptySeats.size());
			throw new NoEnoughSeatsException();
		}
		while(count != 0){
			set.add(emptySeats.pop());
			count--;
		}
		//Add a new name and his/her reserved seats
		reservedSeats.put(name, set);
		return set;
	}
	
	/**
	 * Search the seates reserved by a client.
	 * @param name The name of client.
	 * @return The result.
	 * @throws NoReservationInfoException When cannot find information.
	 */
	public Set<Integer> search(String name) throws NoReservationInfoException{
		System.out.println(name);
		if(reservedSeats.containsKey(name)) {
			return reservedSeats.get(name);
		}else {
			throw new NoReservationInfoException();
		}
	}
	
	/**
	 * Free up seates reserved for a client.
	 * @param name The name of client.
	 * @return The number of seates released.
	 * @throws NoReservationInfoException If cannot find the information of the client.
	 */
	public int[] delete(String name) throws NoReservationInfoException{
		if(reservedSeats.containsKey(name)) {
			int[] num = new int[2];
			num[0] = reservedSeats.get(name).size();
			for(Integer No : reservedSeats.get(name))
				emptySeats.add(No);
			num[1] = emptySeats.size();
			reservedSeats.remove(name);
			assert(!reservedSeats.containsKey(name));
			return num;
		}else {
			throw new NoReservationInfoException();
		}
	}
	
	/**
	 * Return the number of remaining seates.
	 * @return The number of seates.
	 */
	public int remainSeates(){
		return emptySeats.size();
	}
}
