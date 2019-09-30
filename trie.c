#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "trie.h"
#include "invindex.h"
#include <sys/ioctl.h>

#define TRUE 1
#define FALSE 0

void Trie_Init(Trie *trie){
  trie->root = NULL;
}

void insertTextIntoTrie(trieNode **root,char *path){
  FILE *fp ;
  fp = fopen(path,"r");
  char *line = NULL;
  char *line2 = NULL;
  size_t  buf ;
  int row=0;
  char *word=NULL;
  while(getline(&line,&buf,fp)!=-1){
    if(line[strlen(line)-1]=='\n') line[strlen(line)-1]='\0';
    line2 = malloc(sizeof(char)*strlen(line)+1);
    strcpy(line2,line);
    word = strtok(line2," "); //split line into tokens seperated with spacehttp88
    while(word!=NULL){
      Trie_Insert(root,word,path,row);
      word = strtok(NULL," ");
    }
    free(line2);
    line2 = NULL;
    row++;
  }
  fclose(fp);

}
void Trie_Insert(trieNode **root,char *word,char *path,int row){
  /***************************************************
   * input : root of trie and word to be inserted    *
   * function : insert each letter to the right list *
   ***************************************************/
  int i;
  trieNode *temp1 = NULL;
  trieNode *headnode = *root;
  temp1 = List_insert(root,word[0]); //insert the first letter at list of first letters

  for(i=1;i<strlen(word);i++){
    //temp1 = Node for letter word[i]
    temp1 = List_insert(&(temp1->rest),word[i]); //add to this list the i-th letter
  }
  invIndex_Update(&(temp1->invIndex),path,row);
}

trieNode *List_insert(trieNode **head,char letter){
  /************************************************
  *      inserts a letter into the list           *
  *  1.if the letter exists already in the list , *
  *    a pointer to this node is returned         *
  * 2.else the new node is being inserted and     *
  *     returned                                  *
  ************************************************/
  trieNode *tempNode = NULL;
  trieNode *current = *head;
  trieNode *prev = NULL;

  if((*head)==NULL || letter<(*head)->c){
    //if its the first element of the list or
    //letter is of smaller value than head of the list,insert at start
    tempNode = *head;
    *head = malloc(sizeof(trieNode));
    (*head)->c = letter;
    (*head)->otherLetter = tempNode;
    (*head)->rest = NULL;
    (*head)->invIndex = NULL;
    return(*head);
  }else{
    /* else find the right place for the letter */
    while( current !=NULL && current->c<=letter){
      if(current->c == letter){   //if letter is in list , return the node
        return current;
      }
      prev = current;                 //else go to the next node
      current = current->otherLetter;
    }
    /*initialize the Node that will be inserted*/
    tempNode = malloc(sizeof(trieNode));
    tempNode->c = letter;
    tempNode->otherLetter = NULL;
    tempNode->rest = NULL;
    tempNode->invIndex = NULL;
    if(current != NULL){   //new node has to be inserted between previous and current node
      prev->otherLetter = tempNode;         //link prev->temp->curr
      tempNode->otherLetter = current;
    }else{            //insert at end
        prev->otherLetter = tempNode;
    }
    return(tempNode);
  }
}

InvertedIndex *Trie_Search(trieNode *head , char *word){
  int i;
  int not_found = TRUE;
  trieNode *temp = head;
  //gia kathe gramma to brisko sti lista kai proxorao sto paidi tis
  for(i=0;i<strlen(word);i++){
    temp  = inList(temp,word[i]);
    if(temp==NULL){
      return NULL;
    }
    if(i<strlen(word)-1){       //letter found in list
        temp = temp->rest;
    }

  }
  return(temp->invIndex);
}

trieNode *inList(trieNode *list,char letter){
  /****************************************************
   * input : 1st node of list , letter to be found    *
   * function : search the list until letter is found *
   * return : pointer to the node with letter         *
   *            if not found return NULL              *
   ****************************************************/

  trieNode *temp = list;
  while(temp!=NULL && temp->c!=letter){
    temp = temp->otherLetter;
  }
  return temp;
}

void printList(trieNode *list){
  while(list!=NULL){
    printf("%c->",list->c );
    list = list->otherLetter;
  }
  printf("NULL\n");
}
void printTree(trieNode *root,int longest_word,int level,char *word){
    int i ;
    if(root == NULL){
        return;
    }
    if(root->invIndex!=NULL){     /*if this  node completes a word print inverted index*/
      word[level-1]=root->c;
      word[level]=0;
      printf("%s ,",word );
      //invIndex_print(root->invIndex);
    }

    if(root->rest!=NULL){
      word[level-1]=root->c;
      word[level]=0;
      printTree(root->rest,longest_word,level+1,word);

    }

    if(root->otherLetter!=NULL){
      //change the last letter to the next possible
      word[level-1]=0;
      printTree(root->otherLetter,longest_word,level,word);
    }
}




int query_word(char *word,char **query,int noQuery){
  /* returns 1 if word is a word of the query
    else returns 0  */
  int i ;
  for(i=0;i<noQuery;i++){
    if(strcmp(word,query[i])==0){
      return 1;
    }
  }
  return 0;
}


void print_spaces(int n){
  for(int i=0;i<n;i++){
    printf(" ");
  }
}

void postingListFree(InvertedIndex *inv){
  if(inv == NULL) return;
  if(inv->postingList ==NULL) return;
  pList *current = inv->postingList;
  pList *next = current->next;
  offsetList *offset = NULL;
  offsetList *next_offset = NULL;
  while(current!=NULL){   /*nodes of list are being freed from the start , keeping the next node*/
    offset = current->offsets;
    next_offset = offset->next;
    while(offset!=NULL){
      free(offset);
      offset=next_offset;
      if(offset!=NULL) next_offset = next_offset->next;
    }
    free(current);
    current = next;
    if(current!=NULL) next = next->next;
  }
}
void TrieFree(trieNode *node ){
  /*it goes recursively at the leaf nodes of the trie , and frees the nodes from bottom to top */
  if(node == NULL )return;
  if(node->invIndex!=NULL){ //if there is inverted index free it
    postingListFree(node->invIndex);
  }

  TrieFree(node->rest);
  TrieFree(node->otherLetter);

  free(node);


}
