/* here are the first two lines that loads the both text files to load them as tables in hdfs/pig storage  werei,j,v orj,k,v be the digits format of a matrix in txt file */
m = LOAD 'M-matrix-small.txt' USING PigStorage(',') AS (i,j,v:double);
n = LOAD 'N-matrix-small.txt' USING PigStorage(',') AS (j,k,v:double);

/* were we all know j be the common factor in both the matrices or table so we are joining both the tables or matrices because pig is an sql variant  */
J = JOIN m BY j , n BY j;

/* O = FOREACH(JOIN m BY j , n BY j) GENERATE m::i AS mr, n::k AS nc, (m::v)*(n::v) AS value; */

/*for evrey j like we did in the mapreduce and scale like key value pair we multiply the values of matrix and store to respective i,k values */
O = FOREACH J GENERATE m::i AS mr, n::k AS nc, (m::v)*(n::v) AS value;
/* grouping them based on i,k such as mr,nc */

/* multiplied = FOREACH(GROUP O BY (mr,nc))GENERATE group.mr as row, group.nc as column, SUM(O.value) AS value;*/
G = GROUP O BY (mr,nc);

/* based on the mr and nc we need to agument/aggregate the values such as (0,0) 2 and (0,0) 3 then it makes (0,0) 5  */

multiplied = FOREACH G GENERATE group.mr as row, group.nc as column, SUM(O.value) AS value;

/* output format */
STORE multiplied INTO 'output' USING PigStorage (',');
