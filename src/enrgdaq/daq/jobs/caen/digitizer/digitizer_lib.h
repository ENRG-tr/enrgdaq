#ifndef DIGITIZER_LIB_H
#define DIGITIZER_LIB_H

#include <stdint.h>
#include "CAENDigitizer.h"

#define CHANNEL_COUNT 8
#define MAX_SAMPLES_PER_CHANNEL 2048
#define EVENT_POOL_SIZE 200

typedef struct
{
    long acq_events;
    long acq_bytes;
    long missed_events;
    long acq_samples;
} AcquisitionStats_t;

typedef void (*waveform_callback_t)(uint8_t *data, size_t len);
typedef void (*acquisition_stats_callback_t)(AcquisitionStats_t *stats);

typedef struct EventDataCopy
{
    uint32_t ChSize[CHANNEL_COUNT];
    uint16_t Waveforms[CHANNEL_COUNT][MAX_SAMPLES_PER_CHANNEL];

    CAEN_DGTZ_EventInfo_t event_info;
    int is_first_in_block;

} EventDataCopy_t;

typedef struct
{
    waveform_callback_t waveform_callback;
    int filter_threshold;
} ProcessingThreadArgs_t;

typedef struct
{
    uint32_t total_size; // Total size in bytes
    uint8_t board_id;
    uint32_t pattern;
    uint8_t channel_mask;
    uint32_t event_counter;
    uint32_t trigger_time_tag;
    uint64_t pc_unix_ms_timestamp;
} EventHeader_t;

typedef struct
{
    uint8_t channel;
    uint16_t sample_index;
    uint16_t value_lsb;
    int16_t value_mv;
} WaveformSample_t;
typedef struct
{
    int handle;
    int is_debug_verbosity;
    int filter_threshold;
    waveform_callback_t waveform_callback;
    acquisition_stats_callback_t stats_callback;
} RunAcquisitionArgs_t;

void run_acquisition(RunAcquisitionArgs_t *args);
void stop_acquisition();

#endif // DIGITIZER_LIB_H