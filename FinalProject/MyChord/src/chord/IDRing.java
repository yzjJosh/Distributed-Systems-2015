package chord;

public class IDRing {
	
	//exclusive
	public static boolean isBetween(long target, long start, long end){
		if(target <0 || start <0 || end < 0) return false;
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
