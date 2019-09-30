# Job-executor
Implementation of job executor who handles processes via fork/exec , named-pipes , signal handling and bash. Job executor is responsible for answering to user queries , using his workers.

Compilation: make
Execution : ./jobExecutor -d docfile -w workers

Format of queries:
/search q1 q2 q3 … qN –d deadline
User searches for files which include q1,q2,..qn words with time limit

/maxcount keyword
User requests the file in which the word "keyword" appears the most.  

/mincount keyword
User requests the file in which the word "keyword" appears the less.  
/wc
Sum of characters , words and lines of all files are printed.

/exit
jobExecutor terminates his workers and user terminates the app.
