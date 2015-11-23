Yu Sun, yusun@utexas.edu ys8797
Zijiang Yang, yangzijiangjosh@gmail.com zy2743
Compilation instructions:
1. Run the MyChord/src/chord/ChordNode.java and FusionBackup/src/backups/FusedRepository.java in Eclipse.(Its purpose is to create .class files)

2. Modify chords.txt and backup.txt, change the ip address and port number of each node.

3. Open the terminal and run the following command:
(1) ./startBackupNodes.sh <# of chord nodes> <index of node> ..
    For example:
    ./startBackupNodes.sh 5 0 1 2
    This command will open 3 backup nodes, indexed 0, 1, 2. And the # of chord nodes is 5.
(2) ./startChords.sh <index of node> ..
    For example:
    ./startChords.sh 0 1 2 3
    This command will open 4 chord nodes, indexed 0, 1, 2, 3.

4. Run the following commands for lookup operations:
(1) put <key> <value>
(2) get <key>
(3) remove <key>
(4) info 
(5) find node <key> //find where the key is stored in Chord.
(6) putfile <file_path> //Put <key, value> pairs from the file automatically.

