#ifndef BUFFER_H
#define BUFFER_H
#include <semaphore.h>

typedef struct Node {
    struct Node* next;
    struct Node* prev;
    char* str;
} Node;

typedef struct LinkedBuffer{
    Node* head;
    Node* tail;
    int size;
    int max;
    int done;
    // pthread_mutex_t mutex;
    // pthread_cond_t more;
    // pthread_cond_t less;
    sem_t pmut;
    sem_t cmut;
    sem_t occupied;
    sem_t empty;
} LinkedBuffer;

LinkedBuffer* createBuffer(int max);

Node* createNode(char *);

int isEmpty(LinkedBuffer* b);

int isFull(LinkedBuffer* b);

void addData(LinkedBuffer* b, char* data);

void printLinkedBuffer(LinkedBuffer*b);

void revPrintLinkedBuffer(LinkedBuffer* b);

char* popData(LinkedBuffer*);

#endif 