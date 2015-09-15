package server;

import java.io.Serializable;

/**
 * Clock is the logical clock in Lamport's Algorithm. If p1 happened before p2, c1 < c2.
 *
 */
public class Clock implements Comparable<Clock>, Serializable{

	private static final long serialVersionUID = 1L;
	
	public final long timeStep;  // The time step of Clock.
	public final int pid;		// The pid of the process.
	
	/**
	 * Create a new logical clock.
	 * @param timeStep The clock count.
	 * @param pid The pid of process.
	 */
	public Clock(long timeStep, int pid){
		this.timeStep = timeStep;
		this.pid = pid;
	}
	
	/**
	 * Compare two logical clocks, break ties using pid.
	 */
	@Override
	public int compareTo(Clock c) {
		if(timeStep < c.timeStep) return -1;
		else if(timeStep > c.timeStep) return 1;
		else return pid - c.pid;
	}

}
