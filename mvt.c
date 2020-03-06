#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

int **result;
int vector_len;

typedef struct {
  char *fileName;
  FILE *fp0, *fp1;
  int id;
  int k;
} args;

void *mapper_thread(void *  a)
{
  printf ("thread started\n"); 
  fflush (stdout); 
  args *thread_args = a;
  thread_args->fp0 = fopen(thread_args->fileName, "r");

  int *vector = malloc(vector_len * sizeof(int));

  for( int j = 0; j < vector_len; j++){
    vector[j] = 0;
  }

  if(thread_args->fp1){
    int index, vector_val;
    while(fscanf(thread_args->fp1, "%d %d", &index, &vector_val) > 0) {
      vector[index - 1] = vector_val;
    }
  }
  rewind(thread_args->fp1);

  int matrix_row, matrix_column, matrix_val;
  while(fscanf(thread_args->fp0, "%d %d %d", &matrix_row, &matrix_column, &matrix_val) > 0){
    result[thread_args->id][matrix_row - 1] += matrix_val * vector[matrix_column - 1];
  }
  rewind(thread_args->fp0);

  pthread_exit(NULL); 
}

void *reducer_thread(void *  a)
{    
  fflush (stdout); 
  args *thread_args = a;
  int result_vec[vector_len];

  for(int i = 0; i < vector_len; i++){
    result_vec[i] = 0;
  }

  for(int i = 0; i < thread_args->k; i++){
    for(int j = 0; j < vector_len; j++){
      if(result[i][j]){
        result_vec[j] += result[i][j];
      }
    }
  }

  for(int i = 0; i < vector_len; i++){
    if(result_vec[i]){
      printf("%d ", result_vec[i]);
      fprintf(thread_args->fp0, "%d %d\n", i + 1, result_vec[i]);
    }
  }
  pthread_exit(NULL); 
}

int main(int argc, char **argv)
{
  char* matrixf_name = argv[1];
  char* vectorf_name = argv[2];
  char* resultf_name = argv[3];
  const int k = atoi(argv[4]);

  if(k > 10){
    printf("K cannot be more than 10.");
    return 0;
  }

  FILE *matrixf, *splitf, *vectorf, *resultf;
  int l = 0; //matrix file length
  vector_len = 0; //vector file length

  char splitf_name[40] = {"splitfile"};
  char chr;

  char line[250];

  //opening input files
  matrixf = fopen(matrixf_name, "r");
  vectorf = fopen(vectorf_name, "r");
  resultf = fopen(resultf_name, "w+");


  /*Counting rows in vector file.
  */
  if(vectorf == NULL){
    printf("Could not open file.");
    return 0;
  }

  chr = getc(vectorf);

  while (chr != EOF)
  {
      if (chr == '\n')
      {
          vector_len = vector_len + 1;
      }
      
      chr = getc(vectorf);
  }

  rewind(vectorf); 

  if(vector_len > 10000){
    printf("Vector size must be less than 10000.");
    return 0;
  }

  /*Counting lines in matrix file
  */

  if(matrixf == NULL){
    printf("Could not open matrix file.");
    return 0;
  }

  //extract a character from file and store in chr
  chr = getc(matrixf);

  while (chr != EOF)
  {
      //count line
      if (chr == '\n')
      {
          l = l + 1;
      }
      //take next character from file.
      chr = getc(matrixf);
  }

  rewind(matrixf); 

  result = malloc(k * sizeof(int *));

  for( int j = 0; j < k; j++){
    result[j] = malloc( vector_len *sizeof(int) );
  }

  const int s = (l / k) + ((l % k) != 0); //s value
  args *fa[k + 1]; //array of thread args structs 

  for(int i = 0; i < k; i++){
    fa[i] = malloc(sizeof(*fa[0])); //creating file structs

    //creating split files
    splitf_name[9] = i + '0';
    splitf = fopen(splitf_name, "w");

    int counter = 0;

    if( splitf) {
      while (counter < s && fgets(line, sizeof line, matrixf) ) //read a line
      {
        counter += 1;
        fputs(line, splitf);
      }
    }
    fclose(splitf);
  }

  pthread_t tid[k]; //array of threads
  pthread_t tid1;
  int status;
  
  //creating mapper threads
  for(int i = 0; i < k; i++){
    splitf_name[9] = i + '0';
    fa[i]->fileName = splitf_name;
    fa[i]->fp0 = splitf;
    fa[i]->fp1 = vectorf;
    fa[i]->id = i;

    status = pthread_create(&tid[i], NULL, (void *)mapper_thread, fa[i]);
    if( status != 0){
      printf("Cannot create thread.");
      exit(0);
    }

    pthread_join(tid[i], NULL);
  }
  fa[k] = malloc(sizeof(*fa[0]));
  fa[k]->fp0 = resultf;
  fa[k]->k = k;
  //create reducer thread
  pthread_create(&tid1, NULL, (void *)reducer_thread, fa[k]);
  pthread_join(tid1, NULL);
  
  exit(0);
}
