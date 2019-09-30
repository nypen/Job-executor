#include  <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include "invindex.h"
#include <dirent.h>
#define MAX_ARGS 10

#define BUFFERSIZE 256
int makePipes(int N){
    /**********************************************
    * creates 2 named piped for each worker      *
    * N: number of workers                        *
    * returns: 1 on success , -1 on failure       *
    ***********************************************/
    char pipe[128];
    int i;
    for(i=0;i<N;i++){
        sprintf(pipe,"Worker2Executor%d",i);
        if(mkfifo(pipe, 0666) == -1){
          if(errno!=EEXIST){
            perror("Make fifo failed.");
            return -1;
          }
        }
        sprintf(pipe,"Executor2Worker%d",i);
        if(mkfifo(pipe, 0666) == -1){
          if(errno!=EEXIST){
            perror("Make fifo failed.");
            return -1;
          }
        }
    }
}
int readInt(int fd){
  int input;
  while(1){
    if(read(fd,&input,sizeof(int))>0){
      return input;
    }
  }
}

char *readString(int fd){
  char *input = malloc(sizeof(char)*BUFFERSIZE);
  while(1){
    if(read(fd,input,BUFFERSIZE)>0){
      return input;
    }
  }
}

int openPipes(int N,int *fdRead,int *fdWrite){

    /**********************************************
    * opens pipes for writing and reading         *
    * N: number of workers                        *
    * returns: 1 on success , -1 on failure       *
    ***********************************************/

  int i;
  char pipe[BUFFERSIZE];
  for(i=0;i<N;i++){
    sprintf(pipe,"Executor2Worker%d",i);
    if((fdWrite[i]=open(pipe, O_WRONLY)) < 0){    /*pipes for writing wait for the read-end to be opened*/
       perror("4 fifo open problem");
       return -1;
    }
    sprintf(pipe,"Worker2Executor%d",i);
    if((fdRead[i]=open(pipe, O_RDONLY | O_NONBLOCK)) < 0){
       perror("3 fifo open problem");
       return 1;
    }
  }
}

InvertedIndex *CombineSearchResults(InvertedIndex **results,int noResults){
  /* this function gets noResults Inverted Indexes and puts them in one*/
  InvertedIndex *result = NULL;
  pList *current = NULL;
  offsetList *offset_list;
  int i;
  for(i=0;i<noResults;i++){ //for every result
    if(results[i] == NULL) continue;
    current = results[i]->postingList;
    while(current!=NULL){
      offset_list = current->offsets;
      while(offset_list!=NULL){ /*parse every row of path in inverted index*/
        ResultsInvertedIndexUpdate(&result,current->path,offset_list->row_offset);
        offset_list = offset_list->next;
      }
      current = current->next;
    }
  }
  return result;
}

void CountTextBWL(char *path,int *bytes,int *words,int *lines){
  int b=0,w=0,l=0;
  FILE *fp;
  char c , prev_c = ' ';
  fp = fopen(path,"r");
  while((c=getc(fp))!=-1){
    b++;
    if(c == ' ' && prev_c!= ' ' && prev_c!='\n') w++;
    else if(c == '\n'){
      l++;
      if(prev_c!='\n' &&prev_c!=' ') w++;
    }

    prev_c = c;
  }
  *bytes = b;
  *words = w;
  *lines = l;
  return;
}
void CountTotalBWL(char **dir_names,int dirs,int *totalBytes,int *totalWords,int *totalLines){
  *totalBytes=0;
  *totalWords=0;
  *totalLines=0;
  int b,w,l,j;
  DIR *dir;
  struct dirent *d;
  char file[BUFFERSIZE];

  for(j=0;j<dirs;j++){
    if((dir=opendir(dir_names[j]))!=NULL){
      while((d = readdir(dir))!=NULL){
          if(strcmp(d->d_name,".")!=0 && strcmp(d->d_name,"..")!=0){
            strcpy(file,dir_names[j]);
            strcat(file,d->d_name);
            CountTextBWL(file,&b,&w,&l);
            *totalBytes+=b;
            *totalWords+=w;
            *totalLines+=l;
          }
          memset(file,'\0',256);
      }
      closedir(dir);
    }
  }
}
