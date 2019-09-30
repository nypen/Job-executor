#include  <sys/types.h>
#include  <sys/stat.h>
#include  <fcntl.h>
#include  <stdio.h>
#include  <stdlib.h>
#include  <string.h>
#include  <errno.h>
#include <unistd.h>
#include <sys/wait.h>
#include "functions.h"
#include <dirent.h>
#include "parentMessages.h"
#include "childMessages.h"
#include "trie.h"

#define BUFFERSIZE 256
#define MAX_ARGS 10

int main(int argc , char **argv){

  char *docfile;
  int numWorkers , noDirs ;
  pid_t pid;
  int i ,status , options;
  FILE *fp;
  char *line = NULL;
  size_t  buf ;
  char **directories;

  if(argc<5){
    perror("Wrong arguments given. Please execute as:\n\t./jobExecutor –d docfile –w numWorkers\n");
    exit(1);
  }

  for(i=1;i<argc;i=i+2){
    if(!strcmp(argv[i],"-d")){
      docfile = malloc(sizeof(char)*strlen(argv[2])+1);
      strcpy(docfile,argv[i+1]);
    }else if(!strcmp(argv[i],"-w")){
      numWorkers = atoi(argv[i+1]);
    }else{
      perror("Wrong arguments.Please execute as:\n\t./jobExecutor –d docfile –w numWorkers\n");
      exit(1);
    }
  }


  /*reading docfile*/
  fp = fopen(docfile,"r");
  if(fp==NULL){
    perror("Error while opening file.\n");
    exit(1);
  }

  /*counting directories in docfile*/
  noDirs = 0;
  while(getline(&line,&buf,fp)!=-1){
    noDirs++;
  }
  rewind(fp);
  if(noDirs<numWorkers){     //if directories are less than the workers reduce workers
    numWorkers = noDirs;
  }

  directories = malloc(sizeof(char *)*noDirs);
  if(directories==NULL){
    perror("Error while allocating memory.\n");
    exit(1);
  }

  int length ;
  i = 0;
  /*parsing name of paths into array directories*/
  while((length = getline(&line,&buf,fp))!=-1){
    if(line[length-1]=='\n') line[length-1] = '\0'; //excluding \n from line
    directories[i] = malloc(sizeof(char)*length+1);
    strcpy(directories[i],line);
    i++;
  }


    if(makePipes(numWorkers)<0){
      exit(1);
    }

  int worker;
  for(i=0;i<numWorkers;i++){
      pid = fork();
      if(pid == -1){
        perror("Fork failed");
        exit(3);
      }
      worker = i;
      if(pid==0){
          break;
      }
  }
  char pipe[256];
  int *fdWrite = NULL;
  int *fdRead = NULL;
  /*child process*/
  if(pid == 0){

    fdWrite = malloc(sizeof(int));
    fdRead = malloc(sizeof(int));
    /*open pipe for reading */
    sprintf(pipe,"Executor2Worker%d",i);
    if((*fdRead=open(pipe, O_RDONLY |O_NONBLOCK  )) < 0){
      perror("4 fifo open problem"); exit(3);
    }
    /*opening pipe for writing */
    sprintf(pipe,"Worker2Executor%d",i);
    if((*fdWrite=open(pipe, O_WRONLY )) < 0){ /*pipes for writing wait for the read-end to be opened*/
       perror("3 fifo open problem"); exit(3);
    }

    int dirs;
    char command[BUFFERSIZE];
    char **dir_names;
    int j;
    char **queries;
    char temp[BUFFERSIZE];
    InvertedIndex **results;
    trieNode *root = NULL;
    char file[1024];
    char word[256];
    DIR *dir;
    struct dirent *d;
    int max,min,totalBytes,totalWords,totalLines;
    char max_path[BUFFERSIZE],min_path[BUFFERSIZE];
    InvertedIndex *count_result = NULL;

    while(1){
      strcpy(command,readString(*fdRead));
      if(strcmp(command,"DIRNAMES")==0){
        /*read number of directories child will receive*/
        dirs = readInt(*fdRead);
        dir_names = getDirectories(dirs,*fdRead);

        for(j=0;j<dirs;j++){
          if((dir=opendir(dir_names[j]))!=NULL){
            while((d = readdir(dir))!=NULL){
                if(strcmp(d->d_name,".")!=0 && strcmp(d->d_name,"..")!=0){
                  strcpy(file,dir_names[j]);
                  strcat(file,d->d_name);
                  insertTextIntoTrie(&root,file);
                }
                memset(file,'\0',256);
            }
            closedir(dir);
          }
        }
        memset(command,'\0',BUFFERSIZE);
        strcpy(command,"READY");
        write(*fdWrite,command,BUFFERSIZE);
      }else if(strcmp(command,"/search")==0){
        int noQueries = readInt(*fdRead);   //reading number of queris
        InvertedIndex *final_result = NULL;

        queries = malloc(sizeof(char *)*noQueries);
        results = malloc(sizeof(InvertedIndex *)*(noQueries));
        for(j=0;j<noQueries;j++){   /*reading words of query*/
              strcpy(temp,readString(*fdRead));
              queries[j] = malloc(sizeof(char)*strlen(temp)+1);
              strcpy(queries[j],temp);
        }
        for(j=0;j<noQueries;j++){
          results[j] = Trie_Search(root,queries[j]);
        }
        final_result = CombineSearchResults(results,noQueries);
        SendSearchResults(final_result,*fdWrite);
        for(j=0;j<noQueries;j++){
          free(queries[j]);
          if(results[j]!=NULL){
            free(results[j]);
          }
        }
      }else if(strcmp(command,"/maxcount")==0){
        strcpy(word,readString(*fdRead));
        count_result = Trie_Search(root,word);
        MaxCount(count_result,&max,max_path);
        write(*fdWrite,&max,sizeof(int));
        if(max!=0){ //if max count returned both path and frequency
          write(*fdWrite,max_path,BUFFERSIZE);
        }
      }else if(strcmp(command,"/mincount")==0){
        strcpy(word,readString(*fdRead));
        count_result = Trie_Search(root,word);
        MinCount(count_result,&min,min_path);
        write(*fdWrite,&min,sizeof(int));
        if(min!=0){ //if max count returned both path and frequency
          write(*fdWrite,min_path,BUFFERSIZE);
        }
      }else if(strcmp(command,"/wc")==0){
        CountTotalBWL(dir_names,dirs,&totalBytes,&totalWords,&totalLines);
        write(*fdWrite,&totalBytes,sizeof(int));
        write(*fdWrite,&totalWords,sizeof(int));
        write(*fdWrite,&totalLines,sizeof(int));
      }else if(strcmp(command,"/exit")==0){
        TrieFree(root);
        for(j=0;j<dirs;j++){
          free(dir_names[j]);
        }
        free(dir_names);
        close(*fdWrite);
        close(*fdRead);
        free(fdRead);
        free(fdWrite);
        exit(1);
      }
    }
  /*parent process*/
  }else{
    fdWrite = malloc(sizeof(int)*numWorkers);
    fdRead = malloc(sizeof(int)*numWorkers);
    char command[BUFFERSIZE];
    int number;

    /*open read and write ends of pipes for every child*/
    for(i=0;i<numWorkers;i++){
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

    sendDirectoryInfo(fdWrite,numWorkers,directories,noDirs);
    for(i=0;i<numWorkers;i++){
      if(strcmp(readString(fdRead[i]),"READY")!=0){
        //programm has to exit
      }
    }
    printf("Command options:\n");
    printf("/search q1 q2 .. q10\n");
    printf("/maxcount q \n");
    printf("/mincount q\n");
    printf("/wc\n");
    printf("/exit\n");
    char **query;
    char *input=NULL;
    int args,found;
    int j , totalBytes,totalWords,totalLines;
    int no,no2,each_max,general_max,each_min,general_min;
    char path[BUFFERSIZE] , max_path[BUFFERSIZE],min_path[BUFFERSIZE];
    int deadline;
    char *line2 = NULL;
    while(1){

      getline(&line,&buf,stdin);
      if(line[(strlen(line))-1] == '\n') line[(strlen(line))-1] = '\0';
      line2=malloc(sizeof(char)*strlen(line)+1);
      strcpy(line2,line);
      input = strtok(line2," ");
      strcpy(command,input);
      for(i=0;i<numWorkers;i++){      //parse command to workers
        write(fdWrite[i],command,BUFFERSIZE);
      }
      if(strcmp(input,"/search")==0){
        args=-1; /*number of arguments for the query , starts from -1 because search will be counted*/
        while(input!=NULL){
          args++;
          if(strcmp(input,"-d")==0){
            input = strtok(NULL , " ");
            if(input == NULL){
              printf("Wrong command. Please try again.\n");
            }else{
              args--;
              deadline = atoi(input);
            }
          }
          input = strtok(NULL , " ");
        }
        /*splitting input tokens again to store them in the array*/
        if(args>MAX_ARGS) args = MAX_ARGS; //max words for search be 10;
        query = malloc(sizeof(char *)*args);

        input=strtok(line," ");
        input=strtok(NULL," "); //ignore first word
        i=0;
        /*allocate and fill the array with the search input*/
        while(i<args && input!=NULL){
          query[i]=NULL;
          query[i]=malloc(sizeof(char)*strlen(input)+1);
          strcpy(query[i],input);
          input=strtok(NULL," ");
          i++;
        }

        /*informing workers about the number of word queries */
        for(i=0;i<numWorkers;i++){
          write(fdWrite[i],&args,sizeof(int));
          for(j=0;j<args;j++){
            memset(command,'\0',BUFFERSIZE);
            strcpy(command,query[j]);
            write(fdWrite[i],command,BUFFERSIZE);
          }
        }
        printf("Waiting for queries..\n");
        sleep(deadline);    //waiting for results
        number = numWorkers;
        found = 0;
        for(i=0;i<numWorkers;i++){
          if(read(fdRead[i],command,BUFFERSIZE)<=0){
            number--;
            continue;
          }
          if(strcmp(command,"NORES")==0) continue;
          no = readInt(fdRead[i]);  //read number of paths
          found = 1;
          for(j=0;j<no;j++){      //for every path read the name
            strcpy(path,readString(fdRead[i]));
            printf("%s : %d \n",path,readInt(fdRead[i]));
            read(fdRead[i],&no2,sizeof(int));
            while(no2!=-1){
              printf("%d , %s \n",no2 , readString(fdRead[i]) );
              read(fdRead[i],&no2,sizeof(int));
            }
          }
        }
        printf("%d/%d workers replied on time\n",number,numWorkers );
        if(found==0) printf("No results found\n");

      }else if(strcmp(input,"/maxcount")==0){
        input = strtok(NULL , " ");
        if(input == NULL){
          printf("Wrong command,please try again.\n");
          continue;
        }
        memset(command,'\0',BUFFERSIZE);
        strcpy(command,input);
        for(i=0;i<numWorkers;i++){  //send to workers the word for maxcount
          write(fdWrite[i],command,BUFFERSIZE);
        }
        found = 0;
        general_max = 0;
        for(i=0;i<numWorkers;i++){
          each_max = readInt(fdRead[i]);
          if(each_max==0) continue;
          found = 1;
          strcpy(path,readString(fdRead[i]));
          if(general_max == 0 || each_max>general_max || (each_max==general_max  && strcmp(path,max_path)<0 )){
            general_max = each_max;
            strcpy(max_path,path);
          }
        }
        if(found==1){
            printf("%s most times in %s\n",command,max_path );
        }else{
          printf("There are no results for your query.\n");
        }

      }else if(strcmp(input,"/mincount")==0){
        input = strtok(NULL , " ");
        if(input == NULL){
          printf("Wrong command,please try again.\n");
          continue;
        }
        memset(command,'\0',BUFFERSIZE);
        strcpy(command,input);
        for(i=0;i<numWorkers;i++){  //send to workers the word for maxcount
          write(fdWrite[i],command,BUFFERSIZE);
        }
        found = 0;
        general_min = 0;
        for(i=0;i<numWorkers;i++){
          each_min = readInt(fdRead[i]);
          if(each_min==0) continue;
          found = 1;
          strcpy(path,readString(fdRead[i]));
          if(general_min == 0 || each_min<general_min || (each_min==general_min  && strcmp(path,min_path)<0 )){
            general_min = each_min;
            strcpy(min_path,path);
          }
        }
        if(found==1){
          printf("%s less times in %s\n",command,min_path );
        }else{
          printf("There are no results for your query.\n");
        }
      }else if(strcmp(input,"/wc")==0){
        totalBytes = 0;
        totalWords = 0;
        totalLines = 0;
        for(i=0;i<numWorkers;i++){
          totalBytes+=readInt(fdRead[i]);
          totalWords+=readInt(fdRead[i]);
          totalLines+=readInt(fdRead[i]);
        }
        printf("Total bytes in files: %d\n",totalBytes);
        printf("Total words in files: %d\n",totalWords);
        printf("Total lines in files: %d\n",totalLines);

      }else if(strcmp(input,"/exit")==0){
        for(i=0;i<numWorkers;i++){
          close(fdRead[i]);
          close(fdWrite[i]);
          sprintf(pipe,"Worker2Executor%d",i);
          remove(pipe);
          sprintf(pipe,"Executor2Worker%d",i);
          remove(pipe);
        }
        free(fdRead);
        free(fdWrite);

        while ((wait(&status)) > 0);
        printf("Bye\n");
        break;
      }else{
        printf("Wrong command.Please try again.\n");
        continue;
      }
      printf("Type another command\n");

    }
  }

}
