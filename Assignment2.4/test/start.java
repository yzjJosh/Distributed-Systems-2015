import java.io.*;
import java.util.*;
import java.net.*;

public class start{

public static void main(String[] args){
int n = Integer.parseInt(args[0]);
for(int i=0; i<n; i++)
try{
	new Socket(InetAddress.getLocalHost().getHostAddress(), 45678+i);
}
catch(Exception e){
e.printStackTrace();
}


}
}