echo $#
for (( i=1; i <= $#; ++i ))
do 
    p=$(pwd)
    s="osascript -e 'tell app \"Terminal\" to do script \"cd $p&&java -cp MyChord/bin:FusionBackup/bin chord.ChordNode chords.txt backup.txt ${!i}\"'"
    eval $s
    sleep 0.2
done
