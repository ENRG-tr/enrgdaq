#include <stdlib.h>
#include "queue.h"
#include "digitizer_lib.h"

void queue_init(ThreadSafeQueue_t *q)
{
    q->head = NULL;
    q->tail = NULL;
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->cond, NULL);
    q->shutdown = 0;
}

void queue_push_ptr(ThreadSafeQueue_t *q, EventDataCopy_t *data_ptr)
{
    QueueItem_t *item = (QueueItem_t *)malloc(sizeof(QueueItem_t));
    item->data = data_ptr;
    item->next = NULL;

    pthread_mutex_lock(&q->mutex);
    if (q->tail)
    {
        q->tail->next = item;
    }
    else
    {
        q->head = item;
    }
    q->tail = item;

    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mutex);
}

EventDataCopy_t *queue_pop_ptr(ThreadSafeQueue_t *q)
{
    pthread_mutex_lock(&q->mutex);

    while (!q->head && !q->shutdown)
    {
        pthread_cond_wait(&q->cond, &q->mutex);
    }

    if (q->shutdown && !q->head)
    {
        pthread_mutex_unlock(&q->mutex);
        return NULL;
    }

    QueueItem_t *item = q->head;
    if (item)
    {
        q->head = item->next;
        if (!q->head)
        {
            q->tail = NULL;
        }
    }
    pthread_mutex_unlock(&q->mutex);

    if (!item)
        return NULL; // Should not happen if logic is correct

    EventDataCopy_t *data_ptr = item->data;
    free(item); // free the queue item, NOT the data it points to
    return data_ptr;
}

void queue_shutdown(ThreadSafeQueue_t *q)
{
    pthread_mutex_lock(&q->mutex);
    q->shutdown = 1;
    pthread_cond_broadcast(&q->cond);
    pthread_mutex_unlock(&q->mutex);
}
