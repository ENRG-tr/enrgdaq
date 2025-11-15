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

EventDataCopy_t g_event_pool[EVENT_POOL_SIZE];

ThreadSafeQueue_t g_free_pool_queue;
ThreadSafeQueue_t g_work_queue;

void check_dgtz_error(CAEN_DGTZ_ErrorCode ret, const char *func_name)
{
    if (ret == CAEN_DGTZ_Success)
        return;
    const char *error_message = CAEN_DGTZ_GetErrorString(ret);
    if (error_message)
        fprintf(stderr, "Error in %s: %s (%d)\n", func_name, error_message, ret);
    else
        fprintf(stderr, "Error in %s: %d", func_name, ret);
    fflush(stderr);
    exit(1);
}

size_t filter_channel_waveforms(FilterWaveformsArgs_t args)
{
    size_t sample_count = 0;

    struct timeval tv;
    gettimeofday(&tv, NULL);

    uint64_t pc_unix_ms_timestamp =
        (uint64_t)(tv.tv_sec) * 1000 +
        (uint64_t)(tv.tv_usec) / 1000;

    for (int ch = 0; ch < CHANNEL_COUNT; ch++)
    {
        for (int i = 0; i < args.event_copy->ChSize[ch]; i++)
        {
            if (args.event_copy->Waveforms[ch][i] < args.filter_threshold)
                continue;

            if (args->out_buffer->len + sample_count >= args->out_buffer_max_samples)
            {
                fprintf(stderr, "Buffer overflow at filter_channel_waveforms for channel %d!\n", ch);
                fflush(stderr);
                return sample_count;
            }
            uint32_t buf_index = args.out_buffer->len + sample_count;

            // Header
            args.out_buffer->pc_unix_ms_timestamp[buf_index] = pc_unix_ms_timestamp;
            args.out_buffer->event_counter[buf_index] = args.event_copy->event_info->EventCounter;
            args.out_buffer->trigger_time_tag[buf_index] = args.event_copy->event_info->TriggerTimeTag;

            args.out_buffer->channel[buf_index] = (uint8_t)ch;
            args.out_buffer->sample_index[buf_index] = (uint16_t)i;
            args.out_buffer->value_lsb[buf_index] = args.event_copy->Waveforms[ch][i];
            // we're using vx1751 which is 1 v_pp, which is represented using uint16, so we need to map it
            float normalized_value_lsb = (float)args.event_copy->Waveforms[ch][i] / 1023.0;
            float dc_offset_diff = 0; // todo: change this // (float)(65535 - args.channel_dc_offsets[ch]) / 65535.0;
            args.out_buffer->value_mv[buf_index] = (int16_t)((normalized_value_lsb - dc_offset_diff) * 1000.0);

            sample_count++;
        }
    }

    return sample_count;
}

// Updated processing thread
void *processing_thread_func(void *arg)
{
    RunAcquisitionArgs_t *args = (RunAcquisitionArgs_t *)arg;

    WaveformSamples_t acq_buffer = {
        .pc_unix_ms_timestamp = malloc(ACQ_BUFFER_SIZE * sizeof(uint64_t)),
        .event_counter = malloc(ACQ_BUFFER_SIZE * sizeof(uint32_t)),
        .trigger_time_tag = malloc(ACQ_BUFFER_SIZE * sizeof(uint32_t)),
        .channel = malloc(ACQ_BUFFER_SIZE * sizeof(uint8_t)),
        .sample_index = malloc(ACQ_BUFFER_SIZE * sizeof(uint16_t)),
        .value_lsb = malloc(ACQ_BUFFER_SIZE * sizeof(uint16_t)),
        .value_mv = malloc(ACQ_BUFFER_SIZE * sizeof(int16_t)),
        .len = 0};

    AcquisitionStats_t stats = {0};

    time_t last_log_time = time(NULL);

    if (g_is_debug_verbosity)
        fprintf(stdout, "Consumer thread started.\n");

    while (1)
    {
        EventDataCopy_t *item = queue_pop_ptr(&g_work_queue);
        if (item == NULL) // Shutdown
            break;

        stats.acq_events++;

        uint32_t total_ch_size = 0;
        for (int ch = 0; ch < CHANNEL_COUNT; ch++)
            total_ch_size += item->ChSize[ch];

        // Check if buffer is full
        if (acq_buffer.len + total_ch_size > ACQ_BUFFER_SIZE)
        {
            if (g_is_debug_verbosity)
            {
                fprintf(stdout, "Consumer buffer full with %zu items.\n", acq_buffer.len);
            }
            args->waveform_callback(&acq_buffer);
            acq_buffer.len = 0;
        }

        size_t sample_count = filter_channel_waveforms(
            (FilterWaveformsArgs_t){item, args->filter_threshold, args->channel_dc_offsets, &acq_buffer, ACQ_BUFFER_SIZE});

        stats.acq_samples += sample_count;
        acq_buffer.len += sample_count;

        queue_push_ptr(&g_free_pool_queue, item);

        if (time(NULL) - last_log_time >= 1)
        {
            args->stats_callback(&stats);
            stats = (const AcquisitionStats_t){0};
            last_log_time = time(NULL);
        }
    }

    fprintf(stdout, "Consumer thread shutting down.\n");
    free(acq_buffer.channel);
    free(acq_buffer.sample_index);
    free(acq_buffer.value_lsb);
    free(acq_buffer.value_mv);
    free(acq_buffer.pc_unix_ms_timestamp);
    free(acq_buffer.event_counter);
    free(acq_buffer.trigger_time_tag);
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
        fprintf(stdout, "Producer: Pre-allocated %d event buffers.\n", EVENT_POOL_SIZE);

    pthread_t consumer_thread;
    if (pthread_create(&consumer_thread, NULL, processing_thread_func, args) != 0)
    {
        fprintf(stderr, "Failed to create consumer thread.\n");
        fflush(stderr);
        exit(1);
    }

    ret = CAEN_DGTZ_AllocateEvent(args->handle, (void **)&event16);
    check_dgtz_error(ret, "CAEN_DGTZ_AllocateEvent");
    ret = CAEN_DGTZ_MallocReadoutBuffer(args->handle, &buffer, &buffer_size);
    check_dgtz_error(ret, "CAEN_DGTZ_MallocReadoutBuffer");

    // Get DC offsets
    int channel_dc_offsets[CHANNEL_COUNT];
    for (int ch = 0; ch < CHANNEL_COUNT; ch++)
    {
        CAEN_DGTZ_GetChannelDCOffset(args->handle, ch, &channel_dc_offsets[ch]);
        fflush(stdout);
    }
    args->channel_dc_offsets = channel_dc_offsets;

    g_running = 1;
    ret = CAEN_DGTZ_SWStartAcquisition(args->handle);
    check_dgtz_error(ret, "CAEN_DGTZ_SWStartAcquisition");

    if (args->is_debug_verbosity)
        fprintf(stdout, "Producer thread (acquisition) started.\n");

    while (g_running)
    {
        uint32_t read_buffer_size = 0;
        ret = CAEN_DGTZ_ReadData(args->handle, CAEN_DGTZ_SLAVE_TERMINATED_READOUT_MBLT, buffer, &read_buffer_size);
        if (ret != CAEN_DGTZ_Success)
        {
            check_dgtz_error(ret, "CAEN_DGTZ_ReadData");
            continue;
        }

        if (read_buffer_size == 0)
        {
            usleep(100);
            continue;
        }

        uint32_t num_events;
        ret = CAEN_DGTZ_GetNumEvents(args->handle, buffer, read_buffer_size, &num_events);
        check_dgtz_error(ret, "CAEN_DGTZ_GetNumEvents");

        for (int i = 0; i < num_events; i++)
        {
            CAEN_DGTZ_EventInfo_t event_info;
            char *event_ptr = NULL;
            ret = CAEN_DGTZ_GetEventInfo(args->handle, buffer, read_buffer_size, i, &event_info, &event_ptr);
            check_dgtz_error(ret, "CAEN_DGTZ_GetEventInfo");

            ret = CAEN_DGTZ_DecodeEvent(args->handle, event_ptr, (void **)&event16);
            check_dgtz_error(ret, "CAEN_DGTZ_DecodeEvent");

            EventDataCopy_t *item_copy = queue_pop_ptr(&g_free_pool_queue);
            if (item_copy == NULL)
            {
                fprintf(stderr, "ERROR: Failed to allocate event copy!\n");
                fflush(stderr);
                continue;
            }

            item_copy->is_first_in_block = (i == 0);
            item_copy->event_info = event_info; // Struct copy

            // Copy waveform into the event copy
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
                    memcpy(item_copy->Waveforms[ch], event16->DataChannel[ch], ch_size * sizeof(uint16_t));
            }

            queue_push_ptr(&g_work_queue, item_copy);
        }
    }
}

void stop_acquisition()
{
    g_running = 0;
}
