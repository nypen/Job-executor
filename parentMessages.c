#include  <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>

#define BUFFERSIZE 256

void sendDirectoryInfo(int *fd,int n,char **directories,int noDirs){
  int i;
  char string[BUFFERSIZE];

  /*each worker will be responsible for #dirs4workers directories
    if more != 0 , the #more first workers will be responsible for #dirs4workers+1 directories */

  int dirs4workers = noDirs / n;
  int more = noDirs%n;

  memset(string,'\0',BUFFERSIZE);
  strcpy(string,"DIRNAMES");
  for(i=0;i<n;i++){
    write(fd[i],string,BUFFERSIZE);
  }
  for(i=0;i<n;i++){
    //write(fdWrite[i],&NUM_OF_DIRS,sizeof(int));
    if(i<more){ /*the first #more workers will receive #dirs4workers+1 directories*/
        dirs4workers++;
        write(fd[i],&dirs4workers,sizeof(int));
        dirs4workers--;
    }else{
      write(fd[i],&dirs4workers,sizeof(int));
    }
  }

  for(i=0;i<noDirs;i++){
    memset(string,'\0',BUFFERSIZE);
    strcpy(string,directories[i]);
    write(fd[i%n],string,BUFFERSIZE);
  }

}
