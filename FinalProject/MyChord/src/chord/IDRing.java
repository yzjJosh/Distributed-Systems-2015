package chord;

public class IDRing {
	
	//exclusive
	public static boolean isBetween(long target, long start, long end){
		if(start < end){
			if(target > start && target < end) return true;
			else return false;
		}else if(start > end){
			if(target > start || target < end) return true;
			else return false;
		}else
			return target != start;
	}
	
}
