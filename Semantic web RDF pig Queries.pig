-- CLUSTER OF 20 medium INSTANCES with 1 medium master
-- TOTAL RUNTIME WAS 16min 40 sec

register s3n://uw-cse344-code/myudfs.jar

raw = LOAD 's3n://uw-cse344/btc-2010-chunk-000' USING TextLoader as (line:chararray); 

ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);

-- filter on rdfabout.com
filtered_triples = FILTER ntriples BY subject MATCHES '.*rdfabout\\\\.com.*';


-- make copy of tuples
filtered_triples_copy  = 
FOREACH filtered_triples GENERATE subject as subject2, predicate as predicate2, object as object2;

-- join
joined_triples = JOIN filtered_triples BY object, filtered_triples_copy BY subject2;

-- return distinct triples\
joined_triples_distinct = DISTINCT joined_triples;

-- order by predicate
results = ORDER joined_triples_distinct BY predicate;

-- store results
store results into '/user/hadoop/results' using PigStorage();