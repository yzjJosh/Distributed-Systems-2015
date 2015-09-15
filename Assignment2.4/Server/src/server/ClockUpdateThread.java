package server;

/**
 * ClockUpdateThread is a thread witch peiodically sends a timesteped message to all other processes.
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
				//Broadcast(TYPE_CLOCK_MESSAGE, new Message(Server.getClock()), pid);
				
				Thread.sleep(period);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
