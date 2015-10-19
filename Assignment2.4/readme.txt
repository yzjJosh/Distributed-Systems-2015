Authors:
UTEID: zy2743 Name: Zijiang Yang Email: yangzijiangjosh@gmail.com
UTEID: ys8797 Name: Yu Sun 	 Email: yusun@utexas.com

Complile: javac server\Server.java
          javac client\Client.java

Run: 
     java server.Server <Path of server information file> <Maximum number of seats in the theater>
     java client.Client <Path of server information file>

Restrictions: Servers should start one by one. They may not work if they start at the same time.


server information file format:
ip portNum \n

An example of this: #servers.txt#
192.168.1.120 42345
192.168.1.120 42346
192.168.1.120 42347
192.168.1.120 42348
192.168.1.120 42349