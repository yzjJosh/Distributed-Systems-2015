echo $#
for (( i=2; i <= $#; ++i ))
do 
    p=$(pwd)
    s="osascript -e 'tell app \"Terminal\" to do script \"cd $p&&java -cp FusionBackup/bin:FusionBackup/lib/jblas-1.2.4.jar backups.FusedRepository backup.txt ${!i} $1\"'"
    eval $s
    sleep 0.2
done
