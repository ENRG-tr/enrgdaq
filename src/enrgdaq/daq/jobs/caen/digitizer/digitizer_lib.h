#ifndef DIGITIZER_LIB_H
#define DIGITIZER_LIB_H

#include <stdint.h>
#include "CAENDigitizer.h"

#define CHANNEL_COUNT 8
#define MAX_SAMPLES_PER_CHANNEL 2048
#define EVENT_POOL_SIZE 200

typedef struct EventDataCopy
{
    uint32_t ChSize[CHANNEL_COUNT];
    uint16_t Waveforms[CHANNEL_COUNT][MAX_SAMPLES_PER_CHANNEL];

    CAEN_DGTZ_EventInfo_t event_info;
    int is_first_in_block;

} EventDataCopy_t;

typedef void (*python_callback_t)(uint16_t *data, size_t len);

void run_acquisition(int handle, int is_debug_verbosity, int filter_threshold, python_callback_t callback);
void stop_acquisition();

#endif // DIGITIZER_LIB_H