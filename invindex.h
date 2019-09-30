#ifndef INVINDEX_H
#define INVINDEX_H

#define BUFFERSIZE 256

struct offsetList{
	int row_offset;	/*indicates rows where word  is found,every node is the offset row from the start of file*/
	struct offsetList *next;
};
typedef struct offsetList offsetList;

/*struct for posting List*/
struct pList{
  char *path;      /*text id*/
  int fr;      /*frequency of word in path text*/
	offsetList *offsets;
  offsetList *offsets_last;
  struct pList *next;
};

typedef struct pList pList;

struct InvertedIndex{
	int df;
  pList *last;
  pList *postingList;   /*pointer to pList*/
};

typedef struct InvertedIndex InvertedIndex;
void invIndex_print(InvertedIndex *invindex);
void invIndex_Update(InvertedIndex **,char *,int);
void ResultsInvertedIndexUpdate(InvertedIndex **invIndex,char *path,int row);
void MaxCount(InvertedIndex *invindex, int *max_freq , char *max_path);
void MinCount(InvertedIndex *invindex, int *min_freq , char *min_path);

#endif
