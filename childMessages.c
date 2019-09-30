#include  <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include "functions.h"
#include "invindex.h"

#define BUFFERSIZE 256


char **getDirectories(int n,int fd){
  char name[BUFFERSIZE];
  char **directory_names;
  int j;
  directory_names = malloc(sizeof(char *)*n);

  if(directory_names == NULL){
    perror("Error while allocating memory");
    exit(2);
  }

  for(j=0;j<n;j++){
    strcpy(name,readString(fd));
    directory_names[j] = malloc(sizeof(char)*strlen(name)+1);
    if(directory_names[j]==NULL){
      perror("Error while allocating memory");
      exit(2);
    }
    strcpy(directory_names[j],name);
    memset(name,'\0',BUFFERSIZE);
  }
  return directory_names;
}

void SendSearchResults(InvertedIndex *results,int fd){

  offsetList *offset = NULL;
  char path[BUFFERSIZE];
  char row_text[BUFFERSIZE];
  char *line=NULL;
  size_t buf;
  FILE *fp;
  int row;
  memset(row_text,'\0',BUFFERSIZE);
  if(results==NULL){
    strcpy(row_text,"NORES");
    write(fd,row_text,BUFFERSIZE);
    return;
  }else{
    strcpy(row_text,"RES_EXIST");
    write(fd,row_text,BUFFERSIZE);
  }
  pList *postlist = results->postingList;
  write(fd,&(results->df),sizeof(int));   /*send the number of paths father will receive*/
  while(postlist!=NULL){
    strcpy(path,postlist->path);
    write(fd,path,BUFFERSIZE);    /*send path*/
    write(fd,&(postlist->fr),sizeof(int));  /*send number of rows*/
    fp = fopen(path,"r");

    row = 0;
    offset = postlist->offsets;
    /*while file hasnt ended and there are still offsets to send*/
    while(getline(&line,&buf,fp)!=-1 && offset!=NULL){
      if(offset->row_offset == row){
        write(fd,&row,sizeof(int)); //send offset of row
        strcpy(row_text,line);
        write(fd,row_text,BUFFERSIZE);  //and content
        memset(row_text,'\0',BUFFERSIZE);
        offset = offset->next;
      }
      row++;
      line = NULL;
    }
    row = -1;
    write(fd,&row,sizeof(int));   //inform father we are done with that path
    fclose(fp);
    memset(path,'\0',BUFFERSIZE);
    postlist = postlist->next;
  }

}
