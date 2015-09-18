package server;

import java.util.*;

import exceptions.*;

/**
 * TheaterService deals with core functions of seate reservation service.
 *
 */
public class TheaterService {
	
	private String[] seats; //The seates information, each element is a name.
	private HashMap<String, Set<Integer>> reservedSeats; //Name to reserved seates.
	Stack <Integer> emptySeats = new Stack <Integer>();
	
	public TheaterService(int numOfSeats) {
		seats = new String[numOfSeats];
		for(int i = 1; i <= numOfSeats; i++){
			emptySeats.push(i);
		}
	}
	/**
	 * Reserve certain number of seats for client.
	 * @param name The name of client.
	 * @param count The number of seats to reserve.
	 * @return The reserved seats number.
	 * @throws NoEnoughSeatesException When there is no enough seats.
	 * @throws RepeateReservationException When the client has already reserved seats.
	 */
	public Set<Integer> reserve(String name, int count) throws NoEnoughSeatesException, RepeateReservationException{

		Set<Integer> set = new HashSet <Integer> ();
		//If the client has already reserved seats, then throws an exception
		if(reservedSeats.containsKey(name)) {
			throw new RepeateReservationException();
		}
		//If there is no enough seats, then throws an exception
		if(count > emptySeats.size()){
			throw new NoEnoughSeatesException();
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
	public int delete(String name) throws NoReservationInfoException{
		if(reservedSeats.containsKey(name)) {
			int num = reservedSeats.get(name).size();
			reservedSeats.remove(name);
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
		return 0;
	}
}
