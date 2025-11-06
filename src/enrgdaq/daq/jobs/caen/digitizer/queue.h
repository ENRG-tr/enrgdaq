#ifndef QUEUE_H
#define QUEUE_H

#include <pthread.h>

struct EventDataCopy;

typedef struct QueueItem
{
    struct EventDataCopy *data;
    struct QueueItem *next;
} QueueItem_t;

typedef struct
{
    QueueItem_t *head;
    QueueItem_t *tail;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int shutdown;
} ThreadSafeQueue_t;

void queue_init(ThreadSafeQueue_t *q);
void queue_push_ptr(ThreadSafeQueue_t *q, struct EventDataCopy *data_ptr);
struct EventDataCopy *queue_pop_ptr(ThreadSafeQueue_t *q);
void queue_shutdown(ThreadSafeQueue_t *q);

#endif // QUEUE_H