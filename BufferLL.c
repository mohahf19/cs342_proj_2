#include "BufferLL.h"
#include <stdio.h> 
#include <stdlib.h> 
#include <string.h>

LinkedBuffer* createBuffer(int max){
    LinkedBuffer* b = (LinkedBuffer *) malloc(sizeof(LinkedBuffer));
    b->head = NULL;
    b->tail = NULL;
    b->size = 0;
    b->max = max;
    b->done = 0;
    
    // pthread_mutex_init(&b->mutex, NULL);
    // pthread_cond_init(&b->more, NULL);
    // pthread_cond_init(&b->less, NULL);

    sem_init(&b->occupied, 0, 0);
    sem_init(&b->empty, 0, b->max);
    sem_init(&b->pmut, 0, 1);
    sem_init(&b->cmut, 0, 1);

    return b;
}

Node* createNode(char* data){
    Node* n = (Node *) malloc(sizeof(Node));
    n->prev = NULL;
    n->next = NULL;
    n->str = data;

    return n;
}

int isEmpty(LinkedBuffer* b){
    return b->size == 0;
}

int isFull(LinkedBuffer* b){
    return b->size == b->max;
}

void addData(LinkedBuffer* b, char* data){
    //create new string (to deep copy).
    char* copy = malloc(strlen(data) + 1);
    strcpy(copy, data);
    if (!isFull(b)){
        Node* node = createNode(copy);
        if (isEmpty(b)){
            //head and tail are the same
            b->head = node;
            b->tail = node;
        } else{
            b->tail->next = node;
            node->prev = b->tail;
            b->tail = node;
        }
        b->size = b->size + 1;
    } else {
        printf("Cannot add to buffer as it is full\n");
    }
}

void printLinkedBuffer(LinkedBuffer *b){
    Node* t = b->head;
    int counter = 0;
    while (t != NULL){
        printf("[%s] -> ", t->str);
        counter++;
        t = t->next;
    }
    printf("\n");
}

void revPrintLinkedBuffer(LinkedBuffer * b ){
    Node*t = b->tail;
    int counter = b->size -1;
    while(t != NULL){
        printf("Node %d: %s\n", counter, t->str);  
        counter--;
        t = t->prev;
    }
}

char* popData(LinkedBuffer* b){
    char* ret = NULL;
    if (b->size > 0){
        Node* t = b->head;
        ret = t->str;
        b->head = t->next;
        b->size = b->size -1;
        if (b->size > 0)
            b->head->prev = NULL;
        free(t);
    } else {
        printf("cannot pop as buffer is empty\n");
    }
    return ret;
}