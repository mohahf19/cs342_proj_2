#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <wait.h>
#include <math.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/time.h>
#include <semaphore.h>
#include "BufferLL.h"

#define JOIN(a, b) (a ## b)
// WARNING: don't LOOP in the same line
#define loop(n) for (unsigned JOIN(HIDDEN, __LINE__) = 0; JOIN(HIDDEN, __LINE__) < n; JOIN(HIDDEN, __LINE__)++)

// GLOBAL VARIABES

int * result;
int * vector;
int size;
typedef struct mapperPassingData{
          int index;
          LinkedBuffer* b;
} mapperPassingData;

typedef struct reducerPassingData{
  int files;
  LinkedBuffer** buffers;
} reducerPassingData;

void * mapperRunner (void *  a)
{
  
  struct mapperPassingData *data = a;
  int* index = data->index;
  LinkedBuffer* b = data->b;
  //printf ("thread started: %d\n",index); 

  //open the indexth split file
  char buf[255];
  snprintf(buf, 255, "split%d", index);
  FILE *split = fopen(buf, "r");

  char*line = NULL;
    size_t len = 0;
    ssize_t read ;

    //printf("beginning to read..\n");
    int row, col, val;
    while ((read = getline(&line, &len, split) != -1)) {
        
        sscanf(line, "%d%d%d\n", &row, &col, &val);
        int res = (val * vector[col-1]); //to row

        snprintf(buf, 255, "%d %d", row, res);
        //TODO write buf to b after syncronizing it

        //crit section
        pthread_mutex_lock(&b->mutex);
        
        //while buffer is full, wait
        while (isFull(b)){
            pthread_cond_wait(&b->less, &b->mutex);
        }

        //mutex unlocked and there is space in the buffer
        addData(b, buf);

        //signal that we have one more element
        pthread_cond_signal(&b->more);
        
        //unlock the mutex
        pthread_mutex_unlock(&b->mutex);
    }

    //reading here is done! Now set done to 1 to signify that the 
    //mapper's loading process has finished
    pthread_mutex_lock(&b->mutex);
    b->done = 1;
    pthread_mutex_lock(&b->mutex);
    

    pthread_exit(0);  //thread done byebye
}

void* reducerRunner(void* a){
    printf("reducer thread started boi\n");

    struct reducerPassingData *data = a;
    int files = data->files;
    LinkedBuffer** buffers = data->buffers;
    int* done = initEmptyArr(files);
    int countDone = 0;
    while(countDone < files){ // for each buffer
        for(int i = 0; i < files; i++){ 
            //if this buffer is done, skip it
            if (done[i])
                continue; 
            //lock it
            LinkedBuffer* b = buffers[i];
            //lock the mutex
            pthread_mutex_lock(&b->mutex);
            
            //if it is empty, mark it as empty and skip in the future
            if (b->done && isEmpty(b)){
                done[i] = 1;
            } else { //buffer is not done! 
                //while it is empty, wait until there is a new item
                while (isEmpty(b)){
                    pthread_cond_wait(&b->more, &b->mutex);
                }

                //now we have a new element in the buffer, mutex is unlocked
                char* data = popData(b);

                //now signal that there is less # of elements in buffer
                pthread_cond_signal(&b->less);

                //now unlock the mutex
                pthread_mutex_unlock(&b->mutex);

                //TODO process the data
                printf("Reducer read: %s", data);

            }
            
        }
    }
    

    free(done);
    pthread_exit(0);
}

int microsec(const struct timeval *start, const struct timeval *end) {
    long sec = end->tv_sec - start->tv_sec; //to avoid overflow
    long microseconds = ((sec * 1000000) + end->tv_usec) - start->tv_usec;
    return microseconds;
}

int countLines(char *matrixfile);

void createSplits(char *matrixfile, int s, int k , int l, int *);

void createAndProcessSplits(int k, char*, int);

int* mapperProcess(int i, char *vectorfile, int*);

int *readVector(char *vectorfile, int *numLines);

int * initEmptyArr(int n);

void printResult(int *arr, int n, int i, char *);

void combineAndWriteResults(int created, char *resultfile, char* vec);

void deleteMiddleFiles(int created);

void writeToPipe(int* res, int n,int i);

int main(int argc, char *argv[]) {

    if (argc == 6){

        struct timeval before, after;
        gettimeofday(&before, NULL);

        char* matrixfile = argv[1];
        char* vectorfile = argv[2];
        char* resultfile = argv[3];
        char* partitions = argv[4];
        int B;
        int k, s;
        int filesCreated;
        //printf("%s\n%s\n%s\n%s\n", matrixfile, vectorfile, resultfile, partitions);


        int lineCount = countLines(matrixfile);
        //printf("%s has %d lines!\n", matrixfile, lineCount);
        sscanf(partitions, "%d", &k);
        sscanf(argv[5], "%d", &B);
        if (B < 100 || B > 10000){
            printf("Error, B must be between 100 and 10000.\n");
            exit(-1);
        }
        s = lineCount / k;
        createSplits(matrixfile, s, k, lineCount, &filesCreated);

        // read the vector file into the global variable
        vector = readVector(vectorfile, &size);
        result = initEmptyArr(size);
        printf("vector is read and is of size %d\n", size);
        
        createAndProcessSplits(filesCreated, resultfile, B);

        //combineAndWriteResults(filesCreated, resultfile, vectorfile);

        deleteMiddleFiles(filesCreated);

        gettimeofday(&after, NULL);
        printf("mvt took %d microseconds.\n\n", microsec(&before, &after));

    } else{
        //printf("missing parameters!\n");
    }
    return 0;
}

void deleteMiddleFiles(int created) {
    for(int i = 0; i < created; i++){
        char buf[255];
        snprintf(buf, 255, "split%d", i);
        remove(buf);
    }

    //printf("Done deleting! bybye.\n");

}


void combineAndWriteResults(int created, char* resultName, char* vector) {
    pid_t n = 0;

    n = fork();
    if (n < 0){
        //printf("Fork failed:( \n");
        exit(-1);
    } else if (n == 0){ //reducer process
        int n = countLines(vector);
        int * result = initEmptyArr(n);

        for (int i = 0; i < created; i++){
            char buf[255];
            snprintf(buf, 255, "inter%d", i);
            FILE *inter = fopen(buf, "r");

            char* line;
            size_t len = 0;
            ssize_t read;

            while( (read = getline(&line, &len, inter) != -1)){
                int row, val;
                sscanf(line, "%d%d\n", &row, &val);
                result[row-1] += val;
            }
            fclose(inter);
        }
        printResult(result, n, -1, resultName);
        exit(0);
    } //child end

    wait(NULL);
    //printf("Writing result done! Thanks for using meeee. \n");
}

void createAndProcessSplits(int files, char* result, int maxBuffer) {
    //files-many mappers are ready boi
    pthread_t * mappers = malloc(sizeof(pthread_t)* files);
    pthread_attr_t attr;
    pthread_t reducer;
    int** partialResults = malloc(sizeof(int*)*files);
    struct mapperPassingData* data = malloc(sizeof(struct mapperPassingData)*files);
    pid_t n;
    int vectorRow = size;

    // BUFFERS
    // THEY ALREADY HAVE THEIR OWN MUTEXES INSIDE, JUST INITIALZIE THEM.
    LinkedBuffer** buffers = malloc(sizeof(LinkedBuffer * )* files);

    //printf("i need this many veccies.. %d\n", vectorRow);
    int* resultArray =  initEmptyArr(vectorRow);
    //printf("res is ");

    for(int i = 0; i < files; i++){
        data[i].index = i;
        buffers[i] = createBuffer(maxBuffer);
        data[i].b = buffers[i];
        pthread_attr_init(&attr);
        pthread_create(&(mappers[i]), &attr, mapperRunner, &data[i]);
    }

    reducerPassingData d;
    d.files = files;
    d.buffers = buffers;
    pthread_attr_init(&attr);
    pthread_create(&reducer, &attr, reducerRunner, &d);

    pthread_join(reducer, NULL);

    //TODO free the data up (structs for example)
    printResult(resultArray, vectorRow, -1, result);
    //printf("Done writing! lybye\n");
    
}

int* mapperProcess(int index, char *vectorfile, int *arrSize) {
    int * vec;
    int * res;

    vec = readVector(vectorfile, arrSize);
    int n = *arrSize;
    res = initEmptyArr( n);
    //printf("child %d found %d values in vector.\n", index, n);

    char buf[255];
    snprintf(buf, 255, "split%d", index);
    FILE *split = fopen(buf, "r");

    char*line = NULL;
    size_t len = 0;
    ssize_t read ;

    //printf("beginning to read..\n");
    int row, col, val;
    while ((read = getline(&line, &len, split) != -1)) {
        //printf("Read: %s\n", line);
        // for(int i = 0; i < 6; i++){
        //     //printf("|%c|\n", line[i]);
        // }

        
        sscanf(line, "%d%d%d\n", &row, &col, &val);
        res[row-1] = res[row-1] + (val * vec[col-1]);
    }

    fclose(split);

    //printing to files
    //rintf("I WILL MAKE A PIPEEEE\n");
    //writeToPipe(res, n, index);
    free(vec);
    //free(res);
    return res;
    //exit(0);
}

void writeToPipe(int* res, int n,int i){
    //open the ith pipe
    //printf("opening pipe %d\n", i);

    char buf[255];
    snprintf(buf, 255, "./inter%d", i);
    FILE* fd = fopen(buf, "w");
    
    //printf("I AM A CHILD AND IM WRITING\n");
    //TODO seg fault right arounnnddd here
    for(int i = 0; i < n; i++){
        if (i < 0) {
            snprintf(buf, 255, "%d %d\n", i + 1, res[i]);
            fputs(buf, fd);
        } else{
            if (res[i] != 0){
                snprintf(buf, 255, "%d %d\n", i + 1, res[i]);
                fputs(buf, fd);
            }
        }
    }

    fclose(fd);
    unlink(buf);
    //write the array to it
}

void printResult(int *arr, int n, int i, char* filename) {
    //printf("writing to a file %s\n", filename);
    char* buf;
    if ( i >=0 ){ 
        snprintf(buf, 255, "%s%d",filename,  i);
    } else{ //i < 0 is true for the end result file
        buf = filename;
    }

    //printf("file name issss: %s\n", buf);
    FILE *fp = fopen(buf, "w");
    for(int i = 0; i < n; i++){
        if (i < 0) {
            snprintf(buf, 255, "%d %d\n", i + 1, arr[i]);
            fputs(buf, fp);
        } else{
            if (arr[i] != 0){
                snprintf(buf, 255, "%d %d\n", i + 1, arr[i]);
                fputs(buf, fp);
            }
        }
    }

    fclose(fp);
}

int* initEmptyArr(int n) {
    int* arr;
    //printf("initializing array..\n");
    arr = (int *) malloc(sizeof(int) * n);
    for(int i = 0; i < n; i++){
        arr[i] = 0;
    }
    return arr;
}

int *readVector(char *vectorfile, int *numLines) {
    //printf("counting lines..\n");
    int n = countLines(vectorfile);
    *numLines = n;
    //printf("found %d many lines.\n", n);
    //make int arr and start filling it
    int * arr = (int *) malloc(sizeof(int) * (n));
    
    FILE *fp = fopen(vectorfile, "r");

    char*line = NULL;
    size_t len = 0;
    ssize_t read;
    int linesread = 0;

    while ((read = getline(&line, &len, fp) != -1)) {
        int row, val;
        //printf("Read: %s\n", line);
        sscanf(line,"%d%d\n", &row, &val);
        arr[row-1] = val;
        linesread++;
    }
    return arr;
}


void createSplits(char *matrixfile, int s, int k, int l , int* filesCreated) {
    FILE *fp = fopen(matrixfile, "r");
    if (fp == NULL){
        ////printf("Couldn't open %s\n", matrixfile);
    }
    char*line = NULL;
    size_t len = 0;
    ssize_t read;
    int linesread = 0;


    for(int i = 0; i < k; i++){
        if (linesread < l) {
            int sofar = 0;
            char buf[255];
            snprintf(buf, 255, "split%d", i);
            FILE *out = fopen(buf, "w");
            if (out == NULL){
                ////printf("couldn't open file %s\n", buf);
                exit(-1);
            }
            int count = i == k-1 ? s + l % k: s;
            while (sofar < count && (read = getline(&line, &len, fp) != -1)) {
                ////printf("Read: %s\n", line);
                fputs(line, out);
                sofar++;
                linesread++;
            }
            ////printf("done writing %s\n", buf);
            fclose(out);
            *filesCreated = i + 1;
        }
    }

    free(line);
    fclose(fp);
}

int countLines(char *matrixfile) {
    FILE *fp = fopen(matrixfile, "r");
    if (fp == NULL){
        printf("Couldn't find file %s", matrixfile);
        exit(-1);
    }
    int count = 0;

    if (fp == NULL){
        //printf("File %s does not exist.\n", matrixfile);
        exit(-1);
    } else {
        //printf("counting file %s\n", matrixfile);

        char*line = NULL;
        size_t len = 0;

        while ((getline(&line, &len, fp) != -1)) {
            count++;
        }

//
//        while((ch=fgetc(fp)) !=EOF){
//            if (ch == '\n')
//                count++;
//        }
    }

    fclose(fp);
    return count;
}

