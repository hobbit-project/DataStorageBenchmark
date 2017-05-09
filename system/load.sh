#!/bin/bash
ADDRESS=$1
PORT=1111

sleep 1

echo "ld_dir('"$2"', '*.ttl.gz', 'https://github.com/hobbit-project/sparql-snb');" | isql-v $ADDRESS:$PORT
for i in `seq 1 $3`;
do
    isql-v $ADDRESS:$PORT exec="rdf_loader_run()" &
done
wait

echo "xml_set_ns_decl ('snvoc', 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/', 2);"  | isql-v $ADDRESS:$PORT
echo "xml_set_ns_decl ('sn', 'http://www.ldbc.eu/ldbc_socialnet/1.0/data/', 2);"  | isql-v $ADDRESS:$PORT
echo "xml_set_ns_decl ('dbpedia-owl', 'http://dbpedia.org/ontology/', 2);"  | isql-v $ADDRESS:$PORT

echo "checkpoint;" | isql-v $ADDRESS:$PORT
