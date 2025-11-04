
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include "CAENDigitizer.h"
#include "digitizer_lib.h"
#include "queue.h"

static int g_running = 0;

#define ACQ_BUFFER_SIZE 1024 * 128

EventDataCopy_t g_event_pool[EVENT_POOL_SIZE];

ThreadSafeQueue_t g_free_pool_queue;
ThreadSafeQueue_t g_work_queue;

typedef struct
{
    uint16_t *buffer;
    size_t buffer_len;
} FilterResult_t;

typedef struct
{
    uint16_t *data;
    size_t len;
} AcquisitionBuffer_t;

void check_c_error(CAEN_DGTZ_ErrorCode ret, const char *func_name)
{
    if (ret != CAEN_DGTZ_Success)
    {
        fprintf(stderr, "Error in %s: %d", func_name, ret);
        exit(1);
    }
}

size_t filter_channel_waveforms(EventDataCopy_t *event_copy, int threshold, uint16_t *out_buffer, size_t out_buffer_max_len)
{
    size_t buffer_len = 0;
    for (int ch = 0; ch < CHANNEL_COUNT; ch++)
    {
        for (int i = 0; i < event_copy->ChSize[ch]; i++)
        {
            if (event_copy->Waveforms[ch][i] < threshold)
                continue;

            if (buffer_len + 2 > out_buffer_max_len)
                return buffer_len;

            out_buffer[buffer_len++] = i;
            // Use the data from our copied struct
            out_buffer[buffer_len++] = event_copy->Waveforms[ch][i];
        }
    }
    return buffer_len;
}

void *processing_thread_func(void *arg)
{
    python_callback_t callback = (python_callback_t)arg;

    AcquisitionBuffer_t acq_buffer = {
        .data = (uint16_t *)malloc(ACQ_BUFFER_SIZE * sizeof(uint16_t)),
        .len = 0};

    uint16_t temp_filter_buffer[2048];

    long last_event_counter = -1;
    long acq_events = 0;
    long missed_events = 0;
    time_t last_log_time = time(NULL);

    printf("Consumer thread started.\n");

    while (1)
    {
        EventDataCopy_t *item = queue_pop_ptr(&g_work_queue);

        if (item == NULL) // Shutdown
        {
            break;
        }

        acq_events++;

        if (item->is_first_in_block && last_event_counter != -1)
            missed_events += item->event_info.EventCounter - last_event_counter - 1;
        last_event_counter = item->event_info.EventCounter;

        size_t filtered_len = filter_channel_waveforms(item, 750, temp_filter_buffer, 2048);

        if (filtered_len + acq_buffer.len > ACQ_BUFFER_SIZE)
        {
            printf("Consumer buffer full, sending %zu elements.\n", acq_buffer.len);
            uint16_t *data_copy = (uint16_t *)malloc(acq_buffer.len * sizeof(uint16_t));
            memcpy(data_copy, acq_buffer.data, acq_buffer.len * sizeof(uint16_t));
            callback(data_copy, acq_buffer.len);
            acq_buffer.len = 0;
        }

        memcpy(acq_buffer.data + acq_buffer.len, temp_filter_buffer, filtered_len * sizeof(uint16_t));
        acq_buffer.len += filtered_len;

        queue_push_ptr(&g_free_pool_queue, item);

        if (time(NULL) - last_log_time >= 1)
        {
            printf("Consumer Processed %ld events.\n", acq_events);
            printf("Consumer Missed events: %ld\n", missed_events);
            acq_events = 0;
            missed_events = 0;
            last_log_time = time(NULL);
        }
    }

    printf("Consumer thread shutting down.\n");
    free(acq_buffer.data);
    return NULL;
}
void run_acquisition(int handle, python_callback_t callback)
{
    char *buffer = NULL;
    uint32_t buffer_size;
    CAEN_DGTZ_ErrorCode ret;
    CAEN_DGTZ_UINT16_EVENT_t *event16 = NULL;

    queue_init(&g_free_pool_queue);
    queue_init(&g_work_queue);

    for (int i = 0; i < EVENT_POOL_SIZE; i++)
    {
        queue_push_ptr(&g_free_pool_queue, &g_event_pool[i]);
    }
    printf("Producer: Pre-allocated %d event buffers.\n", EVENT_POOL_SIZE);

    pthread_t consumer_thread;
    if (pthread_create(&consumer_thread, NULL, processing_thread_func, callback) != 0)
    {
        fprintf(stderr, "Failed to create consumer thread.\n");
        exit(1);
    }

    ret = CAEN_DGTZ_AllocateEvent(handle, (void **)&event16);
    check_c_error(ret, "CAEN_DGTZ_AllocateEvent");
    ret = CAEN_DGTZ_MallocReadoutBuffer(handle, &buffer, &buffer_size);
    check_c_error(ret, "CAEN_DGTZ_MallocReadoutBuffer");

    g_running = 1;
    ret = CAEN_DGTZ_SWStartAcquisition(handle);
    check_c_error(ret, "CAEN_DGTZ_SWStartAcquisition");

    printf("Producer thread (acquisition) started.\n");

    while (g_running)
    {
        uint32_t read_buffer_size = 0;
        ret = CAEN_DGTZ_ReadData(handle, CAEN_DGTZ_SLAVE_TERMINATED_READOUT_MBLT, buffer, &read_buffer_size);
        if (ret != CAEN_DGTZ_Success)
        {
            check_c_error(ret, "CAEN_DGTZ_ReadData");
            continue;
        }

        if (read_buffer_size == 0)
        {
            usleep(100);
            continue;
        }

        uint32_t num_events;
        ret = CAEN_DGTZ_GetNumEvents(handle, buffer, read_buffer_size, &num_events);
        check_c_error(ret, "CAEN_DGTZ_GetNumEvents");

        for (int i = 0; i < num_events; i++)
        {
            CAEN_DGTZ_EventInfo_t event_info;
            char *event_ptr = NULL;
            ret = CAEN_DGTZ_GetEventInfo(handle, buffer, read_buffer_size, i, &event_info, &event_ptr);
            check_c_error(ret, "CAEN_DGTZ_GetEventInfo");

            ret = CAEN_DGTZ_DecodeEvent(handle, event_ptr, (void **)&event16);
            check_c_error(ret, "CAEN_DGTZ_DecodeEvent");

            EventDataCopy_t *item_copy = queue_pop_ptr(&g_free_pool_queue);
            if (item_copy == NULL)
            {
                continue;
            }

            item_copy->is_first_in_block = (i == 0);
            item_copy->event_info = event_info; // Struct copy

            for (int ch = 0; ch < CHANNEL_COUNT; ch++)
            {
                uint32_t ch_size = event16->ChSize[ch];
                if (ch_size > MAX_SAMPLES_PER_CHANNEL)
                {
                    fprintf(stderr, "ERROR: MAX_SAMPLES_PER_CHANNEL is too small! %u > %d\n", ch_size, MAX_SAMPLES_PER_CHANNEL);
                    // Handle error: stop, skip, etc.
                    ch_size = MAX_SAMPLES_PER_CHANNEL; // Truncate to prevent buffer overflow
                }
                item_copy->ChSize[ch] = ch_size;
                if (ch_size > 0)
                {
                    memcpy(item_copy->Waveforms[ch], event16->DataChannel[ch], ch_size * sizeof(uint16_t));
                }
            }

            // 3. Push the *filled* item to the consumer
            queue_push_ptr(&g_work_queue, item_copy);
        }
    }

    printf("Producer thread stopping...\n");

    queue_shutdown(&g_work_queue);
    queue_shutdown(&g_free_pool_queue);

    pthread_join(consumer_thread, NULL);

    CAEN_DGTZ_FreeReadoutBuffer(&buffer);
    CAEN_DGTZ_FreeEvent(handle, (void **)&event16);

    printf("Acquisition fully stopped.\n");
}

void stop_acquisition()
{
    g_running = 0;
}
