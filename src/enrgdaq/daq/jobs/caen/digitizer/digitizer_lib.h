#ifndef DIGITIZER_LIB_H
#define DIGITIZER_LIB_H

#include <stdint.h>
#include "CAENDigitizer.h"

#define CHANNEL_COUNT 8
#define MAX_SAMPLES_PER_CHANNEL 1024 * 2
#define EVENT_POOL_SIZE 1024 * 4
#define ACQ_BUFFER_SIZE 1024 * 1024 * 8

typedef struct
{
    long acq_events;
    long acq_samples;
} AcquisitionStats_t;

typedef struct
{
    uint32_t len;
    uint64_t *pc_unix_ms_timestamp;
    uint64_t *real_ns_timestamp;
    uint32_t *event_counter;
    uint32_t *trigger_time_tag;
    uint8_t *channel;
    uint16_t *sample_index;
    uint16_t *value_lsb;
    int16_t *value_mv;
} WaveformSamples_t;

typedef void (*waveform_callback_t)(WaveformSamples_t *samples);
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
    int handle;
    int is_debug_verbosity;
    int filter_threshold;
    waveform_callback_t waveform_callback;
    acquisition_stats_callback_t stats_callback;

    int *channel_dc_offsets;
} RunAcquisitionArgs_t;

typedef struct
{
    EventDataCopy_t *event_copy;
    int filter_threshold;
    int *channel_dc_offsets;
    WaveformSamples_t *out_buffer;
    size_t out_buffer_max_samples;
    int64_t pc_unix_ms_timestamp;
    int64_t real_ns_timestamp_without_sample;
} FilterWaveformsArgs_t;

void run_acquisition(RunAcquisitionArgs_t *args);
void stop_acquisition();

#endif // DIGITIZER_LIB_H