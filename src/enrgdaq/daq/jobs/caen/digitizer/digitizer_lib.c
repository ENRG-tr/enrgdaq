
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include "CAENDigitizer.h"
#include "digitizer_lib.h"

static int g_running = 0;
static int CHANNEL_COUNT = 8;
static int ACQ_BUFFER_SIZE = 1024 * 1024;

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

FilterResult_t
filter_channel(CAEN_DGTZ_UINT16_EVENT_t *event16, int threshold)
{
    uint16_t buffer[2048];
    size_t buffer_len = 0;
    for (int ch = 0; ch < CHANNEL_COUNT; ch++)
    {
        // Loop every waveform
        for (int i = 0; i < event16->ChSize[ch]; i++)
        {
            if (event16->DataChannel[ch][i] < threshold)
                continue;
            buffer[buffer_len++] = i;
            buffer[buffer_len++] = event16->DataChannel[ch][i];
            // printf("%i crossed threshold with value %i\n", ch, event16->DataChannel[ch][i]);
        }
    }
    return (FilterResult_t){
        .buffer = buffer,
        .buffer_len = buffer_len};
}

void run_acquisition(int handle, python_callback_t callback)
{
    AcquisitionBuffer_t acq_buffer = {
        .data = (uint16_t *)malloc(ACQ_BUFFER_SIZE * sizeof(uint16_t)),
        .len = 0};

    char *buffer = NULL;
    uint32_t buffer_size;
    CAEN_DGTZ_ErrorCode ret;
    CAEN_DGTZ_UINT16_EVENT_t *event16 = NULL;

    ret = CAEN_DGTZ_AllocateEvent(handle, (void **)&event16);
    check_c_error(ret, "CAEN_DGTZ_AllocateEvent");
    ret = CAEN_DGTZ_MallocReadoutBuffer(handle, &buffer, &buffer_size);
    check_c_error(ret, "CAEN_DGTZ_MallocReadoutBuffer");

    g_running = 1;
    ret = CAEN_DGTZ_SWStartAcquisition(handle);
    check_c_error(ret, "CAEN_DGTZ_SWStartAcquisition");

    long last_event_counter = -1;
    long acq_events = 0;
    long missed_events = 0;
    time_t last_log_time = time(NULL);
    while (g_running)
    {
        uint32_t num_events;
        ret = CAEN_DGTZ_ReadData(handle, CAEN_DGTZ_SLAVE_TERMINATED_READOUT_MBLT, buffer, &buffer_size);
        if (ret != CAEN_DGTZ_Success)
        {
            check_c_error(ret, "CAEN_DGTZ_ReadData");
            continue;
        }

        ret = CAEN_DGTZ_GetNumEvents(handle, buffer, buffer_size, &num_events);
        acq_events += num_events;
        check_c_error(ret, "CAEN_DGTZ_GetNumEvents");

        for (int i = 0; i < num_events; i++)
        {
            CAEN_DGTZ_EventInfo_t event_info;
            char *event_ptr = NULL;
            ret = CAEN_DGTZ_GetEventInfo(handle, buffer, buffer_size, i, &event_info, &event_ptr);
            check_c_error(ret, "CAEN_DGTZ_GetEventInfo");

            ret = CAEN_DGTZ_DecodeEvent(handle, event_ptr, (void **)&event16);
            check_c_error(ret, "CAEN_DGTZ_DecodeEvent");

            // Count events
            if (i == 0 && last_event_counter != -1)
                missed_events += event_info.EventCounter - last_event_counter - 1;
            last_event_counter = event_info.EventCounter;

            FilterResult_t filtered_data = filter_channel(event16, 750);
            if (filtered_data.buffer_len + acq_buffer.len <= ACQ_BUFFER_SIZE)
            {
                // memcpy to acq_buffer
                memcpy(acq_buffer.data + acq_buffer.len, filtered_data.buffer, filtered_data.buffer_len * sizeof(uint16_t));
                acq_buffer.len += filtered_data.buffer_len;
            }
            else
            {
                // Clear buffer and send it
                acq_buffer.len = 0;
                // TODO: add sending code
            }
            // if (callback != NULL)
            //    callback(&event_info, event16);
        }

        if (time(NULL) - last_log_time >= 1)
        {
            printf("Acquired %ld events.\n", acq_events);
            printf("Missed events: %ld\n", missed_events);
            acq_events = 0;
            missed_events = 0;
            last_log_time = time(NULL);
        }

        // sleep 1sec
        // usleep(1000000);
    }
}

void stop_acquisition()
{
    g_running = 0;
}
