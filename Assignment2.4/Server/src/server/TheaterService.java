package server;

import exceptions.*;

/**
 * TheaterService deals with core functions of seate reservation service.
 *
 */
public class TheaterService {
	
	/**
	 * Reserve certain number of seates for client.
	 * @param name The name of client.
	 * @param count The number of seates to reserve.
	 * @return The reserved seats number.
	 * @throws NoEnoughSeatesException When there is not enough seats.
	 * @throws RepeateReservationException When the client has already researved seates.
	 */
	public Iterable<Integer> reserve(String name, int count) throws NoEnoughSeatesException, RepeateReservationException{
		return null;
	}
	
	/**
	 * Search the seates reserved by a client.
	 * @param name The name of client.
	 * @return The result.
	 * @throws NoReservationInfoException When cannot find information.
	 */
	public Iterable<Integer> search(String name) throws NoReservationInfoException{
		return null;
	}
	
	/**
	 * Free up seates reserved for a client.
	 * @param name The name of client.
	 * @return The number of seates released.
	 * @throws NoReservationInfoException If cannot find the information of the client.
	 */
	public int delete(String name) throws NoReservationInfoException{
		return 0;
	}
	
	/**
	 * Return the number of remaining seates.
	 * @return The number of seates.
	 */
	public int remainSeates(){
		return 0;
	}
}
