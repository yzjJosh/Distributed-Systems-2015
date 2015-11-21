package chord;

public class FingerTable {

	private Entry[] fingers;

	public FingerTable(long startID) {
		this.fingers = new Entry[32];
		for (int i = 0; i < fingers.length; i++) {
			long start_id = (startID + (long) Math.pow(2, i)) % (1L<<32); 
			fingers[i] = new Entry(start_id);
			fingers[i].successor = startID;
		}
	}
	
	public synchronized void setSuccessor(int index, long successor){
		fingers[index].successor = successor;
	}
	
	public long getSuccessor(int index){
		return fingers[index].successor;
	}
	
	public long getStart(int index){
		return fingers[index].start;
	}
	
	public long closest_preceding_finger(long id) {
		for (int i = 31; i >= 0; i--) 
			if (IDRing.isBetween(fingers[i].successor, fingers[0].start-1, id))
				return fingers[i].successor;
		return fingers[0].start-1; 
	}
	
	

	private class Entry{
		public final long start;
		public long successor;
		
		public Entry(long start){
			this.start = start;
		}
	}
	
	
	public String toString() {
		String ret = "********---------------------------------------------------------------------***********\n";
		for (int i = 0; i < fingers.length; i++) {
			ret += "start: " + fingers[i].start  + "| successor: " + fingers[i].successor+"\n";
		}
		ret += "********---------------------------------------------------------------------***********\n";
		return ret;
	}
	
	public static void main(String[] args){
		FingerTable table = new FingerTable(34);
		System.out.println(table);
		//Unit test
		for(int i=0; i<10000; i++){
			assert(table.closest_preceding_finger(i) == table.fingers[0].start-1);
		}
		for(int i=0; i<1000; i++){
			int index = (int)(Math.random()*32);
			assert(table.fingers[index].successor == table.fingers[0].start-1);
			table.setSuccessor(index, table.fingers[0].start-1);
			for(int j=0; j<1000; j++){
				long rand = (long)(Math.random()*Integer.MAX_VALUE);
				assert(rand >= 0);
				long should = (rand > table.fingers[index].successor || rand <= table.fingers[0].start-1)? table.fingers[index].successor: (table.fingers[0].start-1);
				assert(table.closest_preceding_finger(rand) == should)
				:"Got "+table.closest_preceding_finger(rand)+", which should be "+should;
			}
			table.setSuccessor(index, table.fingers[0].start-1);
		}
		System.out.println("pass!");
	}
		
		
}
