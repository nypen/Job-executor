#include "invindex.h"
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <string.h>

void invIndex_Update(InvertedIndex **invIndex,char *path,int row){
  /******************************************************************
   *     input : double pointer to the head of the posting list,    *
   *          path of the text                                        *
   *     function : posting list of the word is updated             *
   *     if inverted index is empty -> initiate it                  *
   *   if text in path not in posting list add it at the end    *
   *          if it exists , update frequency and offsetslist                      *
   ******************************************************************/
  pList *current = NULL;

  if((*invIndex) == NULL){    /*inverted index is empty , create inverted index*/
    (*invIndex) = malloc(sizeof(InvertedIndex));
    if(*invIndex == NULL) exit(1);
    (*invIndex)->df = 1;
    (*invIndex)->postingList = malloc(sizeof(pList));
    (*invIndex)->postingList->path = malloc(sizeof(char)*strlen(path)+1);
    strcpy((*invIndex)->postingList->path,path);
    (*invIndex)->postingList->fr = 1;
    (*invIndex)->postingList->next = NULL;
    /*add offset*/
    (*invIndex)->postingList->offsets = malloc(sizeof(offsetList));
    (*invIndex)->postingList->offsets_last = (*invIndex)->postingList->offsets;
    (*invIndex)->postingList->offsets->row_offset = row;
    (*invIndex)->postingList->offsets->next = NULL;
    (*invIndex)->last = (*invIndex)->postingList;
  }else{

    current = (*invIndex)->last;    /*go to the last node of the posting list*/
    (*invIndex)->df++;
    if(strcmp(current->path,path)!=0){     /*if last node is not about the text we insert make a new node*/
      /*and initiate it*/
      current->next = malloc(sizeof(pList));
      current = current->next;
      /*parse path of text, set frequency to 1 , next to NULL */
      current->path = malloc(sizeof(char)*strlen(path)+1);
      strcpy(current->path,path);
      current->fr=1;
      current->next = NULL;
      /*initiate offsets list and add the row*/
      current->offsets = malloc(sizeof(offsetList));
      current->offsets_last = current->offsets;
      current->offsets->row_offset = row;
      current->offsets->next = NULL;
	//ftiaxno kai to current->next->offsets
      (*invIndex)->last = current;
    }else{
      /*if node exists , path found in posting list , frequency is increased*/
      current->fr++;
      /*if last node of offsets != row which we want to insert*/
      if(current->offsets_last->row_offset != row ){
          /*make a new node and parse data , update the pointers*/
          current->offsets_last->next = malloc(sizeof(offsetList));
          current->offsets_last = current->offsets_last->next;
          current->offsets_last->row_offset = row;
          current->offsets_last->next = NULL;
      }//if row already in offsets list do nothing.
    }
  }
}

void invIndex_print(InvertedIndex *invindex){
  if(invindex==NULL) return;
  offsetList *offset = NULL;
  pList * postList = invindex->postingList;
  while(postList!=NULL){
    if(postList->path!=NULL)
      printf("[%s,%d] : ",postList->path,postList->fr );
    offset = postList->offsets;
    while(offset!=NULL){
      printf("%d - ", offset->row_offset);
      offset = offset->next;
    }
    postList = postList->next;
  }
  printf("\n");
}

void ResultsInvertedIndexUpdate(InvertedIndex **invIndex,char *path,int row){
  pList *current = NULL;

  if((*invIndex) == NULL){    /*inverted index is empty , create inverted index*/
    (*invIndex) = malloc(sizeof(InvertedIndex));
    (*invIndex)->df = 1;
    (*invIndex)->postingList = malloc(sizeof(pList));
    (*invIndex)->postingList->path = malloc(sizeof(char)*strlen(path)+1);
    strcpy((*invIndex)->postingList->path,path);
    (*invIndex)->postingList->fr = 1;
    (*invIndex)->postingList->next = NULL;
    /*add offset*/
    (*invIndex)->postingList->offsets = malloc(sizeof(offsetList));
    (*invIndex)->postingList->offsets_last = (*invIndex)->postingList->offsets;
    (*invIndex)->postingList->offsets->row_offset = row;
    (*invIndex)->postingList->offsets->next = NULL;
    (*invIndex)->last = (*invIndex)->postingList;
  }else{
    offsetList *offset = NULL;
    offsetList *previous_offset=NULL;
    pList *previous =NULL;
    current = (*invIndex)->postingList;    /*go to the last node of the posting list*/
    if(strcmp(current->path,path)>0){ //new node at start
      (*invIndex)->df++;
      (*invIndex)->postingList = malloc(sizeof(pList));
      (*invIndex)->postingList->path = malloc(sizeof(char)*strlen(path)+1);
      strcpy((*invIndex)->postingList->path , path);
      (*invIndex)->postingList->fr = 1;
      (*invIndex)->postingList->next = current;
      (*invIndex)->postingList->offsets = malloc(sizeof(offsetList));
      (*invIndex)->postingList->offsets->row_offset = row;
      (*invIndex)->postingList->offsets->next = NULL;
      return;
    }
    while(current!=NULL && strcmp(current->path,path)<=0){
      if(strcmp(current->path,path)==0){ /*if path already in list*/
            offset = current->offsets;
            if(row < offset->row_offset){    //if row has to be inserted at the front
              current->offsets = malloc(sizeof(offsetList));
              current->fr++;
              current->offsets->row_offset = row;
              current->offsets->next = offset;
              return;
            }
            while(offset != NULL && offset->row_offset <= row){   /*add row into offset list*/
                if(offset->row_offset == row) return; //row already in offset list
                previous_offset = offset;
                offset = offset->next;
            }
            /*insert new node and return*/
            current->fr++;
            previous_offset->next = malloc(sizeof(offsetList));
            previous_offset->next->row_offset = row;
            previous_offset->next->next = offset;
            return;
      }
      //else go to the next
      previous = current;
      current = current->next;
      /**/
    }
    /*make new node , insert path and row and return*/
    (*invIndex)->df++;  /*increase the number of paths in posting list*/
    previous->next = malloc(sizeof(pList));
    previous->next->next = current;
    current = previous->next;
    current->fr = 1;
    current->path = malloc(sizeof(char)*strlen(path)+1);
    strcpy(current->path,path);
    current->offsets = malloc(sizeof(offsetList));
    current->offsets->row_offset = row;
    current->offsets->next = NULL;
    return;

  }
}

void MaxCount(InvertedIndex *invindex, int *max_freq , char *max_path){
  if(invindex == NULL){
    *max_freq = 0;      /*if inverted index is null , max_freq is set to 0 and function returns*/
    return;
  }
  pList *postlist = invindex->postingList;
  if(postlist==NULL){
    *max_freq = 0;      /*if postinglist is null , max_freq is set to 0 and function returns*/
    return;
  }else{
    *max_freq = postlist->fr;
    strcpy(max_path,postlist->path);
  }
  while(postlist!=NULL){
    if(postlist->fr > *max_freq || (postlist->fr == *max_freq && strcmp(postlist->path,max_path)<0)){
      *max_freq = postlist->fr;
      strcpy(max_path,postlist->path);
    }
    postlist = postlist->next;
  }
}

void MinCount(InvertedIndex *invindex, int *min_freq , char *min_path){
  if(invindex == NULL){
    *min_freq = 0;      /*if inverted index is null , min_freq is set to 0 and function returns*/
    return;
  }
  pList *postlist = invindex->postingList;
  if(postlist==NULL){
    *min_freq = 0;      /*if postinglist is null , min_freq is set to 0 and function returns*/
    return;
  }else{
    *min_freq = postlist->fr;
    strcpy(min_path,postlist->path);
  }
  while(postlist!=NULL){
    if(postlist->fr < *min_freq || (postlist->fr == *min_freq && strcmp(postlist->path,min_path)<0)){
      *min_freq = postlist->fr;
      strcpy(min_path,postlist->path);
    }
    postlist = postlist->next;
  }
}
