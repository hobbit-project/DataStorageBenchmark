#!/bin/bash
ADDRESS=$1
PORT=1111
FOLDER=$2
NUMBEROFLOADERS=$3
GRAPHURI=$4

sleep 1
echo "delete from load_list;" | isql $ADDRESS:$PORT
isql $ADDRESS:$PORT exec="DB.DBA.RDF_OBJ_FT_RULE_DEL (null, null, 'ALL');;"
echo "ld_dir('"$FOLDER"', '*', '"$GRAPHURI"');" | isql $ADDRESS:$PORT
for i in `seq 1 $NUMBEROFLOADERS`;
do
    isql $ADDRESS:$PORT exec="rdf_loader_run()" &
done
wait

echo "checkpoint;" | isql $ADDRESS:$PORT
