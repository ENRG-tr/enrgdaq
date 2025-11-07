#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

// From the new digitizer code
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

// Structure for a single event
typedef struct
{
    uint8_t board_id;
    uint32_t pattern;
    uint8_t channel_mask;
    uint32_t event_counter;
    uint32_t trigger_time_tag;
    uint64_t pc_unix_ms_timestamp;
    WaveformSample_t *event_data;
    size_t num_samples;
} Event_t;

// Structure for the collection of events
typedef struct
{
    Event_t *events;
    size_t count;
    size_t capacity;
} EventList_t;

// Initialize event list
EventList_t *event_list_create(size_t initial_capacity)
{
    EventList_t *list = (EventList_t *)malloc(sizeof(EventList_t));
    if (!list)
        return NULL;

    list->events = (Event_t *)malloc(initial_capacity * sizeof(Event_t));
    if (!list->events)
    {
        free(list);
        return NULL;
    }

    list->count = 0;
    list->capacity = initial_capacity;
    return list;
}

// Add event to list (with dynamic resizing)
int event_list_add(EventList_t *list, Event_t *event)
{
    if (list->count >= list->capacity)
    {
        size_t new_capacity = list->capacity * 2;
        Event_t *new_events = (Event_t *)realloc(list->events, new_capacity * sizeof(Event_t));
        if (!new_events)
            return -1;

        list->events = new_events;
        list->capacity = new_capacity;
    }

    list->events[list->count++] = *event;
    return 0;
}

// Free event list and all contained data
void event_list_free(EventList_t *list)
{
    if (!list)
        return;

    for (size_t i = 0; i < list->count; i++)
    {
        free(list->events[i].event_data);
    }
    free(list->events);
    free(list);
}

// Parse acquisition buffer
size_t parse_acquisition_buffer(const uint8_t *buffer_bytes, size_t buffer_len, EventList_t **events_out)
{
    EventList_t *events = event_list_create(16); // Start with capacity for 16 events
    if (!events)
    {
        *events_out = NULL;
        return 0;
    }

    size_t offset = 0;

    while (offset < buffer_len)
    {
        // Check if we have enough bytes for a header
        if (offset + sizeof(EventHeader_t) > buffer_len)
        {
            break;
        }

        // Parse header
        EventHeader_t *header = (EventHeader_t *)(buffer_bytes + offset);

        // Check if we have enough bytes for the complete event
        if (offset + header->total_size > buffer_len)
        {
            // Not enough data for the full event
            break;
        }

        // Calculate waveform data size
        size_t data_bytes = header->total_size - sizeof(EventHeader_t);
        size_t num_samples = data_bytes / sizeof(WaveformSample_t);

        WaveformSample_t *event_data = (WaveformSample_t *)malloc(data_bytes);
        if (!event_data)
        {
            event_list_free(events);
            *events_out = NULL;
            return 0; // Error
        }

        // Copy waveform data
        size_t event_data_offset = offset + sizeof(EventHeader_t);
        memcpy(event_data, buffer_bytes + event_data_offset, data_bytes);

        // Create event
        Event_t event = {
            .board_id = header->board_id,
            .pattern = header->pattern,
            .channel_mask = header->channel_mask,
            .event_counter = header->event_counter,
            .trigger_time_tag = header->trigger_time_tag,
            .pc_unix_ms_timestamp = header->pc_unix_ms_timestamp,
            .event_data = event_data,
            .num_samples = num_samples};

        // Add to list
        if (event_list_add(events, &event) != 0)
        {
            free(event_data);
            event_list_free(events);
            *events_out = NULL;
            return 0; // Error
        }

        // Move to next event
        offset += header->total_size;
    }

    *events_out = events;
    return offset;
}

// Write events to CSV file
int write_events_to_csv(const EventList_t *events, FILE *f, size_t *event_counter_ptr)
{
    if (!f)
    {
        fprintf(stderr, "Error: invalid file pointer provided to write_events_to_csv\n");
        return -1;
    }
    // Write data
    size_t event_counter = *event_counter_ptr;
    for (size_t i = 0; i < events->count; i++)
    {
        const Event_t *event = &events->events[i];

        for (size_t j = 0; j < event->num_samples; j++)
        {
            fprintf(f, "%zu,%lu,%u,%u,%u,%u,%u,%u,%u,%u,%hd\n",
                    event_counter++,
                    event->pc_unix_ms_timestamp,
                    event->trigger_time_tag,
                    event->board_id,
                    event->pattern,
                    event->channel_mask,
                    event->event_counter,
                    event->event_data[j].channel,
                    event->event_data[j].sample_index,
                    event->event_data[j].value_lsb,
                    event->event_data[j].value_mv);
        }
    }

    return 0;
}

// Print usage information
void print_usage(const char *program_name)
{
    fprintf(stderr, "Usage: %s <input_file> <output_file>\n", program_name);
    fprintf(stderr, "  <input_file>  - Binary file containing acquisition data\n");
    fprintf(stderr, "  <output_file> - CSV file to write parsed data\n");
}

// Main function
int main(int argc, char *argv[])
{
    if (argc != 3)
    {
        print_usage(argv[0]);
        return 1;
    }

    const char *input_filename = argv[1];
    const char *output_filename = argv[2];

    FILE *f_in = fopen(input_filename, "rb");
    if (!f_in)
    {
        fprintf(stderr, "Error: Could not open input file '%s'\n", input_filename);
        return 1;
    }

    FILE *f_out = fopen(output_filename, "w");
    if (!f_out)
    {
        fprintf(stderr, "Error: Could not open output file '%s'\n", output_filename);
        fclose(f_in);
        return 1;
    }

    // Write CSV header
    fprintf(f_out, "i,pc_unix_ns_timestamp,trigger_time_tag,board_id,pattern,channel_mask,event_counter,channel,sample_index,value_lsb,value_mv\n");

    const size_t BATCH_SIZE = 1024 * 1024 * 100; // 100 MB batch size
    uint8_t *buffer = (uint8_t *)malloc(BATCH_SIZE);
    if (!buffer)
    {
        fprintf(stderr, "Error: Could not allocate memory for buffer\n");
        fclose(f_in);
        fclose(f_out);
        return 1;
    }

    size_t bytes_in_buffer = 0;
    size_t total_events_parsed = 0;
    size_t total_data_points = 0;

    while (1)
    {
        size_t bytes_to_read = BATCH_SIZE - bytes_in_buffer;
        size_t bytes_read = fread(buffer + bytes_in_buffer, 1, bytes_to_read, f_in);

        bytes_in_buffer += bytes_read;

        if (bytes_in_buffer == 0)
        {
            break; // End of file and buffer is empty
        }

        printf("Processing batch of %zu bytes...\n", bytes_in_buffer);

        EventList_t *events = NULL;
        size_t consumed_bytes = parse_acquisition_buffer(buffer, bytes_in_buffer, &events);

        if (!events && consumed_bytes == 0)
        {
            fprintf(stderr, "Error: Failed to parse buffer chunk.\n");
            // If consumed_bytes is 0 and we are not at EOF, we might be in an unrecoverable state.
            if (!feof(f_in))
            {
                fprintf(stderr, "Error: Cannot process data, stopping.\n");
                break;
            }
        }

        if (events)
        {
            printf("Parsed %zu events in this batch\n", events->count);

            size_t batch_data_points = 0;
            for (size_t i = 0; i < events->count; i++)
            {
                batch_data_points += events->events[i].num_samples;
            }
            printf("Batch data points: %zu\n", batch_data_points);
            printf("total_events_parsed: %zu\n", total_events_parsed);

            if (write_events_to_csv(events, f_out, &total_events_parsed) != 0)
            {
                event_list_free(events);
                break; // Stop on write error
            }

            total_data_points += batch_data_points;
            event_list_free(events);
        }

        if (consumed_bytes > 0)
        {
            // Move leftover data to the beginning of the buffer
            bytes_in_buffer -= consumed_bytes;
            memmove(buffer, buffer + consumed_bytes, bytes_in_buffer);
        }

        if (bytes_read == 0 && bytes_in_buffer > 0)
        {
            fprintf(stderr, "Warning: %zu bytes of trailing data in file will be discarded.\n", bytes_in_buffer);
            break;
        }
    }

    printf("\nFinished processing.\n");
    printf("Total events parsed: %zu\n", total_events_parsed);
    printf("Total data points: %zu\n", total_data_points);
    printf("Success! CSV file written to '%s'.\n", output_filename);

    // Cleanup
    free(buffer);
    fclose(f_in);
    fclose(f_out);

    return 0;
}