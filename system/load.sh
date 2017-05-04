#!/bin/bash
ADDRESS=$1
PORT=1111

sleep 1

echo "ld_dir('"$2"', '*.ttl.gz', 'sib');" | isql-v $ADDRESS:$PORT
for i in `seq 1 $3`;
do
    isql-v $ADDRESS:$PORT exec="rdf_loader_run()" &
done
wait
echo "checkpoint;" | isql-v $ADDRESS:$PORT


echo "xml_set_ns_decl ('snvoc', 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/', 2);"  | isql-v $ADDRESS:$PORT
echo "xml_set_ns_decl ('sn', 'http://www.ldbc.eu/ldbc_socialnet/1.0/data/', 2);"  | isql-v $ADDRESS:$PORT
echo "xml_set_ns_decl ('dbpedia-owl', 'http://dbpedia.org/ontology/', 2);"  | isql-v $ADDRESS:$PORT


echo "
create procedure LdbcUpdateSparql (in triplets varchar array)
{
	declare n_dead any;
	n_dead := 0;
	again:	
	declare exit handler for sqlstate '40001' {
		rollback work;
		n_dead := n_dead + 1;
		if (10 < n_dead) {
		   signal ('40001', 'Over 10 deadlocks in rdf load, please retry load');
		   return;
		}
		goto again;
	};
	for vectored
	    (in t varchar := triplets) {
	    ttlp_mt(t, '', 'sib', 0);
	}
	return;
};
" | isql-v $ADDRESS:$PORT

echo "checkpoint;" | isql-v $ADDRESS:$PORT
