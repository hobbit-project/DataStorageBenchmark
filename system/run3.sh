#!/bin/sh

cp /sparql-snb/virtuoso-hobbit-geosparql.lic /opt/virtuoso/bin/virtuoso.lic
#sudo ln -s /lib/x86_64-linux-gnu/libncurses.so.5.9 /lib/x86_64-linux-gnu/libtermcap.so.2
cd /opt/virtuoso/database
cp /sparql-snb/virtuoso.ini.template virtuoso.ini
rm /opt/virtuoso/database/virtuoso.lck
/opt/virtuoso/install/command-oplmgr.sh
virtuoso

# wait until virtuoso is ready
echo $(date +%H:%M:%S.%N | cut -b1-12)" : Waiting until Virtuoso Server is online..."
until grep -m 1 "Server online at 1111" /myvol/db/virtuoso.log
do
  sleep 1
  seconds_passed=$((seconds_passed+1))
  echo $seconds_passed >> out.txt
  if [ $seconds_passed -gt 120 ]; then
    echo $(date +%H:%M:%S.%N | cut -b1-12)" : Could not start Virtuoso Server. Timeout: [2 min]"
    break
  fi
done
echo $(date +%H:%M:%S.%N | cut -b1-12)" : Virtuoso Server started successfully."

cd /sparql-snb
java -cp DataStorageBenchmark.jar org.hobbit.core.run.ComponentStarter1 org.hobbit.sparql_snb.systems.VirtuosoSysAda
