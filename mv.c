#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

int main(int argc, char **argv)
{
  printf ("mv started\n"); 
  char const* const matrixf_name = argv[1];
  char const* const vectorf_name = argv[2];
  char const* const resultf_name = argv[3];
  const int k = atoi(argv[4]);

  if(k > 10){
    printf("K cannot be more than 10.");
    return 0;
  }

  FILE *matrixf, *splitf, *vectorf, *intermediatef, *resultf;
  int l = 0; //matrix file length
  int vector_len = 0; //vector file length

  char intermediatef_name[40] = {"intermediatefile"};
  char splitf_name[40] = {"splitfile"};
  char chr;

  int index, vector_val, matrix_row, matrix_column, matrix_val;

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
          vector_len = vector_len + 1;
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
          l = l + 1;
      //take next character from file.
      chr = getc(matrixf);
  }

  rewind(matrixf); 

  const int s = (l / k) + ((l % k) != 0);

  for(int i = 0; i < k; i++){
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

  for (int i = 0; i < k; i++) {
    pid_t id;
    id = fork();

    if (id < 0) {
      printf("\nFork Failed");
      exit(-1);
    } 
    else if (id == 0) { //child process
      splitf_name[9] = i + '0';
      splitf = fopen(splitf_name, "r");

      int result[vector_len]; 
      int *vector = malloc(vector_len * sizeof(int));

      for( int j = 0; j < vector_len; j++){
        vector[j] = 0;
        result[j] = 0;
      }

      if(vectorf){
        while(fscanf(vectorf, "%d %d", &index, &vector_val) > 0) {
          vector[index - 1] = vector_val;
        }
      }
      rewind(vectorf);

      while(fscanf(splitf, "%d %d %d", &matrix_row, &matrix_column, &matrix_val) > 0){
        result[matrix_row - 1] += matrix_val * vector[matrix_column - 1];
      }

      fclose(splitf);
      
      intermediatef_name[16] = i + '0';
      intermediatef = fopen(intermediatef_name, "w+");

      for(int j = 0; j < vector_len; j++){
          if(result[j]){
            fprintf(intermediatef, "%d %d\n", j + 1, result[j]);
          }
      }

      fclose(intermediatef);
      exit(0);
    }
  }

  //main process waiting for the child processes to finish executions
  for (int i = 0; i < k; i++)
  {
      wait(NULL);
  }

  //creating another child
  int result_vec[vector_len];
  pid_t id;
  id = fork();

  if (id < 0) {
    printf("\nFork Failed");
    exit(-1);
  } 
  else if (id == 0) { //child process
    for(int i = 0; i < vector_len; i++){
        result_vec[i] = 0;
    }

    for(int i = 0; i < k; i++){
      intermediatef_name[16] = i + '0';
      intermediatef = fopen(intermediatef_name, "r");

      while(fscanf(intermediatef, "%d %d", &index, &vector_val) > 0){
        result_vec[index - 1] += vector_val;
      }
      fclose(intermediatef);
    }
    
    for(int i = 0; i < vector_len; i++){
      if(result_vec[i]){
        fprintf(resultf, "%d %d\n", i + 1, result_vec[i]);
      }
    }
    fclose(resultf);
    exit(0);
  }
  else{
    wait(NULL);
  }
  exit(0);
}
