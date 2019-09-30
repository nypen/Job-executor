#include "invindex.h"

int makePipes(int);
int openPipes(int,int *,int *);
char *readString(int );
InvertedIndex *CombineSearchResults(InvertedIndex **results,int noResults);
void CountTotalBWL(char **dir_names,int dirs,int *totalBytes,int *totalWords,int *totalLines);
void CountTextBWL(char *path,int *bytes,int *words,int *lines);
int readInt(int );
