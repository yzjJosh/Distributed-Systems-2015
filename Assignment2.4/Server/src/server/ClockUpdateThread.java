package server;

/**
 * ClockUpdateThread is a thread witch peiodically sends a timestamped message to all other processes.
 *
 */
public class ClockUpdateThread extends Thread {

	private final int period;		//The period(ms) in which messages are sent.
	
	/**
	 * Create a new ClockUpdateThread with a certain sending period.
	 * @param period The period(ms) in which a message is sent to all other processes.
	 */
	public ClockUpdateThread(int period){
		this.period = period;
	}
	
	@Override
	public void run(){
		try {
			while(true){
				Server.broadCastClock();				
				Thread.sleep(period);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
