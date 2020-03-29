// #ifndef BUFFER_H
// #define BUFFER_H

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
} LinkedBuffer;

LinkedBuffer* createBuffer(int max);

Node* createNode(char *);

int isEmpty(LinkedBuffer* b);

int isFull(LinkedBuffer* b);

void addData(LinkedBuffer* b, char* data);

void printLinkedBuffer(LinkedBuffer*b);

void revPrintLinkedBuffer(LinkedBuffer* b);

char* popData(LinkedBuffer*);

// #endif 