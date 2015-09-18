package server;

import java.util.concurrent.Semaphore;

/**
 * ReaderWriterLock provides protection for resources which can be write by only one thread but read by multiple threads. Multiple readers
 * can share a lock, but only one writer can get the lock.
 */
public class ReaderWriterLock{
	
	private int CocurrentReaderNum;
	private Semaphore access;
	private Semaphore mutex;
	
	/**
	 * Create a ReaderWriterLock which can be shared by multiple readers.
	 * @param CocurrentReaderNum The maximum number of readers which can share this lock. This parameter shoulb be larger than 0.
	 * @throws IllegalArgumentException if CocurrentReaderNum is less than 1;
	 */
	public ReaderWriterLock(int CocurrentReaderNum){
		if(CocurrentReaderNum < 1) 
			throw new IllegalArgumentException("Illegal argument: "+CocurrentReaderNum);
		this.CocurrentReaderNum = CocurrentReaderNum;
		mutex = new Semaphore(1);
		access = new Semaphore(CocurrentReaderNum);
	}
	
	/**
	 * Create a ReaderWriterLock which can be shared by 10 readers.
	 */
	public ReaderWriterLock(){
		this(10);
	}
	
	/**
	 * Reader get the lock. If this lock is got by any writer or got by more than maximum number of readers, this thread will block
	 * until the lock is avaliable.
	 * @throws InterruptedException If the thread is interrupted when waiting for the lock
	 */
	public void ReaderAcquire() throws InterruptedException{
		access.acquire();
	}
	
	/**
	 * Reader release the lock. If the next thread waiting for this lock is reader, then the waiting reader can aquire this lock. If
	 * the next thread waiting for this lock is writer and no reader currently holding the lock, then the writer can acquire it. If the next thread
	 *  waiting for this lock is writer and there are still other readers holding this lock, the writer will keep waiting. 
	 */
	public void ReaderRelease(){
		access.release();
	}
	
	/**
	 * Writer get the lock. If this lock is got by another writer or reader, this thread will block until the lock is completely free.
	 * @throws InterruptedException If the thread is interrupted when waiting for the lock
	 */
	public void WriterAcquire() throws InterruptedException{
		mutex.acquire();
		access.acquire(CocurrentReaderNum);
	}
	
	/**
	 * Writer release the lock. The next reader or writer waiting for this lock can get it.
	 */
	public void WriterRelease(){
		access.release(CocurrentReaderNum);
		mutex.release();
	}
	
	/**
	 * Return the maximum number of readers which can share this lock
	 * @return the maximum number of readers which can share this lock
	 */
	public int maxCocurrentReaders(){
		return this.CocurrentReaderNum;
	}
	
}
