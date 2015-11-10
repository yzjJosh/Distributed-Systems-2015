package backups;

import java.util.concurrent.Semaphore;

/**
 * WriterReaderLock is a lock which accepts (more than 1 readers and 0 writers) or 
 * (0 readers and only 1 writers) at the same time.
 * @author Josh
 *
 */
public class WriterReaderLock {
	
	private final Semaphore s;
	private final int maxReader;
	
	public WriterReaderLock(int maxReader){
		s = new Semaphore(maxReader);
		this.maxReader = maxReader;
	}
	
	public void readerLock(){
		try {
			s.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void readerUnlock(){
		s.release();
	}
	
	public void writerLock(){
		try {
			s.acquire(maxReader);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void writerUnlock(){
		s.release(maxReader);
	}
	
}
