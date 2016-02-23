register s3n://uw-cse344-code/myudfs.jar

raw = LOAD 's3n://uw-cse344' USING TextLoader as (line:chararray); 

ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);

-- group the n-triples by subject
subjects = group ntriples by subject;

-- flatten
count_by_subject = foreach subjects generate flatten($0), COUNT($1) as count;

-- group by count
count_subjects = group count_by_subject by count;

-- flatten 
count_by_count_subjects = foreach count_subjects generate flatten($0), COUNT($1) as frequency;

-- order by frequency
count_by_count_subjects_ordered = order count_by_count_subjects by (frequency) desc;

-- store results
store count_by_count_subjects_ordered into '/user/hadoop/results' using PigStorage();
