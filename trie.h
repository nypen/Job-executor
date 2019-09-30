
#ifndef TRIE_H
#define TRIE_H
#include "invindex.h"

struct trieNode{
  char c;             /* Letter store in this node*/
  struct trieNode *otherLetter;   /* Pointer to other letters that have the same prefix*/
  struct trieNode *rest;         /* Pointer to words that have the letter c */
  InvertedIndex *invIndex;     /* Inverted index for word with final letter the letter c*/
};

typedef struct trieNode trieNode;

struct Trie{
  trieNode *root;
};

typedef struct Trie Trie;


void Trie_Init(Trie *);
trieNode *List_insert(trieNode **,char );
void printList(trieNode *);
void insertTextIntoTrie(trieNode **root,char *path);
void Trie_Insert(trieNode **,char *,char*,int);
trieNode *inList(trieNode *,char);
InvertedIndex *Trie_Search(trieNode * , char *);
void printTree(trieNode *,int ,int,char *);
void print_spaces(int n);
char *fill_with_spaces(int n);
int query_word(char *word,char **query,int noQuery);
void TrieFree(trieNode *root );
#endif
