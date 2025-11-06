#define _POSIX_C_SOURCE 199309L
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <inttypes.h>
#include "CAENDigitizer.h"
#include "digitizer_lib.h"
#include "queue.h"

static int g_running = 0;
static int g_is_debug_verbosity = 0;

#define ACQ_BUFFER_SIZE 1024 * 512
#define FILTER_BUFFER_SIZE 1024 * 64
#define HEADER_SIZE 6
#define PROCESSING_THREAD_TEMP_BUFFER_SIZE 1024 * 5

EventDataCopy_t g_event_pool[EVENT_POOL_SIZE];

ThreadSafeQueue_t g_free_pool_queue;
ThreadSafeQueue_t g_work_queue;

void check_c_error(CAEN_DGTZ_ErrorCode ret, const char *func_name)
{
    if (ret != CAEN_DGTZ_Success)
    {
        fprintf(stderr, "Error in %s: %d", func_name, ret);
        exit(1);
    }
}

size_t filter_channel_waveforms(EventDataCopy_t *event_copy, int threshold,
                                WaveformSample_t *out_buffer, size_t out_buffer_max_samples)
{
    size_t sample_count = 0;

    for (int ch = 0; ch < CHANNEL_COUNT; ch++)
    {
        for (int i = 0; i < event_copy->ChSize[ch]; i++)
        {
            if (event_copy->Waveforms[ch][i] < threshold)
                continue;

            if (sample_count >= out_buffer_max_samples)
            {
                fprintf(stderr, "Buffer overflow at filter_channel_waveforms for channel %d!\n", ch);
                fflush(stderr);
                return sample_count;
            }

            out_buffer[sample_count].channel = (uint16_t)ch;
            out_buffer[sample_count].sample_index = (uint16_t)i;
            out_buffer[sample_count].value_lsb = event_copy->Waveforms[ch][i];
            // We're using VX1751 which is 1 V_pp, which is represented using uint16, so we need to map it
            // like => 0--1023 => (-500)--499
            float normalized_value_lsb = (float)event_copy->Waveforms[ch][i] / 1023.0;
            out_buffer[sample_count].value_mv = (int16_t)(normalized_value_lsb * 1000.0) - 500;
            sample_count++;
        }
    }

    return sample_count;
}

// Updated processing thread
void *processing_thread_func(void *arg)
{
    fflush(stdout);
    RunAcquisitionArgs_t *args = (RunAcquisitionArgs_t *)arg;

    uint8_t *acq_buffer = (uint8_t *)malloc(ACQ_BUFFER_SIZE);
    size_t acq_buffer_len = 0;

    WaveformSample_t temp_filter_buffer[9999];
    long last_event_counter = -1;
    AcquisitionStats_t stats = {0};

    time_t last_log_time = time(NULL);

    if (g_is_debug_verbosity)
        printf("Consumer thread started.\n");

    while (1)
    {
        EventDataCopy_t *item = queue_pop_ptr(&g_work_queue);
        if (item == NULL) // Shutdown
            break;

        stats.acq_events++;
        if (item->is_first_in_block && last_event_counter != -1)
            stats.missed_events += item->event_info.EventCounter - last_event_counter - 1;
        last_event_counter = item->event_info.EventCounter;

        // Build timestamp
        struct timespec spec;
        clock_gettime(CLOCK_MONOTONIC, &spec);
        uint64_t pc_unix_ns_timestamp = spec.tv_sec * 10e9 + spec.tv_nsec;

        // Build header struct
        EventHeader_t header = {
            .board_id = item->event_info.BoardId,
            .pattern = item->event_info.Pattern,
            .channel_mask = item->event_info.ChannelMask,
            .event_counter = item->event_info.EventCounter,
            .trigger_time_tag = item->event_info.TriggerTimeTag,
            .pc_unix_ns_timestamp = pc_unix_ns_timestamp};

        // Filter waveforms into structured buffer
        fflush(stdout);
        size_t sample_count = filter_channel_waveforms(item, args->filter_threshold, temp_filter_buffer, 9999);
        stats.acq_samples += sample_count;

        size_t header_bytes = sizeof(EventHeader_t);
        size_t data_bytes = sample_count * sizeof(WaveformSample_t);
        size_t total_bytes = header_bytes + data_bytes;

        header.total_size = total_bytes;
        stats.acq_bytes += total_bytes;

        // Check if buffer is full
        if (acq_buffer_len + total_bytes > ACQ_BUFFER_SIZE)
        {
            fflush(stdout);
            if (g_is_debug_verbosity)
                printf("Consumer buffer full, sending %zu bytes.\n", acq_buffer_len);

            uint8_t *data_copy = (uint8_t *)malloc(acq_buffer_len);
            memcpy(data_copy, acq_buffer, acq_buffer_len);
            fflush(stdout);
            args->waveform_callback(data_copy, acq_buffer_len);
            acq_buffer_len = 0;
        }

        // Dump structs to bytes - crystal clear!
        memcpy(acq_buffer + acq_buffer_len, &header, header_bytes);
        acq_buffer_len += header_bytes;

        memcpy(acq_buffer + acq_buffer_len, temp_filter_buffer, data_bytes);
        acq_buffer_len += data_bytes;

        queue_push_ptr(&g_free_pool_queue, item);

        if (time(NULL) - last_log_time >= 1)
        {
            /*
            if (g_is_debug_verbosity)
            {
                printf("Consumer Thread Acquisition Stats: Events=%" PRIu64 ", Bytes=%" PRIu64 ", Missed Events=%" PRIu64 ", Samples=%" PRIu64 "\n",
                       stats.acq_events, stats.acq_bytes, stats.missed_events, stats.acq_samples);
                fflush(stdout);
            }*/
            args->stats_callback(&stats);
            stats = (const AcquisitionStats_t){0};
            last_log_time = time(NULL);
        }
    }

    printf("Consumer thread shutting down.\n");
    free(acq_buffer);
    return NULL;
}

void run_acquisition(RunAcquisitionArgs_t *args)
{
    g_is_debug_verbosity = args->is_debug_verbosity;

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
    if (args->is_debug_verbosity)
        printf("Producer: Pre-allocated %d event buffers.\n", EVENT_POOL_SIZE);

    pthread_t consumer_thread;
    if (pthread_create(&consumer_thread, NULL, processing_thread_func, args) != 0)
    {
        fprintf(stderr, "Failed to create consumer thread.\n");
        exit(1);
    }

    ret = CAEN_DGTZ_AllocateEvent(args->handle, (void **)&event16);
    check_c_error(ret, "CAEN_DGTZ_AllocateEvent");
    ret = CAEN_DGTZ_MallocReadoutBuffer(args->handle, &buffer, &buffer_size);
    check_c_error(ret, "CAEN_DGTZ_MallocReadoutBuffer");

    g_running = 1;
    ret = CAEN_DGTZ_SWStartAcquisition(args->handle);
    check_c_error(ret, "CAEN_DGTZ_SWStartAcquisition");

    if (args->is_debug_verbosity)
        printf("Producer thread (acquisition) started.\n");

    while (g_running)
    {
        uint32_t read_buffer_size = 0;
        ret = CAEN_DGTZ_ReadData(args->handle, CAEN_DGTZ_SLAVE_TERMINATED_READOUT_MBLT, buffer, &read_buffer_size);
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
        ret = CAEN_DGTZ_GetNumEvents(args->handle, buffer, read_buffer_size, &num_events);
        check_c_error(ret, "CAEN_DGTZ_GetNumEvents");

        for (int i = 0; i < num_events; i++)
        {
            CAEN_DGTZ_EventInfo_t event_info;
            char *event_ptr = NULL;
            ret = CAEN_DGTZ_GetEventInfo(args->handle, buffer, read_buffer_size, i, &event_info, &event_ptr);
            check_c_error(ret, "CAEN_DGTZ_GetEventInfo");

            ret = CAEN_DGTZ_DecodeEvent(args->handle, event_ptr, (void **)&event16);
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
                    fflush(stderr);
                    ch_size = MAX_SAMPLES_PER_CHANNEL;
                }
                item_copy->ChSize[ch] = ch_size;
                if (ch_size > 0)
                {
                    memcpy(item_copy->Waveforms[ch], event16->DataChannel[ch], ch_size * sizeof(uint16_t));
                }
            }

            queue_push_ptr(&g_work_queue, item_copy);
        }
    }
}

void stop_acquisition()
{
    g_running = 0;
}
