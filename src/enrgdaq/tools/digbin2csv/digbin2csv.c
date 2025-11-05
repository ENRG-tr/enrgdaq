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
EventList_t *parse_acquisition_buffer(const uint8_t *buffer_bytes, size_t buffer_len)
{
    EventList_t *events = event_list_create(16); // Start with capacity for 16 events
    if (!events)
        return NULL;

    size_t offset = 0;

    while (offset < buffer_len)
    {
        // Check if we have enough bytes for a header
        if (offset + sizeof(EventHeader_t) > buffer_len)
        {
            fprintf(stderr, "Warning: Incomplete header at offset %zu, stopping parse\n", offset);
            break;
        }

        // Parse header
        EventHeader_t *header = (EventHeader_t *)(buffer_bytes + offset);

        // Check if we have enough bytes for the complete event
        if (offset + header->total_size > buffer_len)
        {
            fprintf(stderr, "Warning: Incomplete event at offset %zu, stopping parse\n", offset);
            break;
        }

        // Calculate waveform data size
        size_t data_bytes = header->total_size - sizeof(EventHeader_t);
        size_t num_samples = data_bytes / sizeof(WaveformSample_t);

        WaveformSample_t *event_data = (WaveformSample_t *)malloc(data_bytes);
        if (!event_data)
        {
            event_list_free(events);
            return NULL;
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
            .event_data = event_data,
            .num_samples = num_samples};

        // Add to list
        if (event_list_add(events, &event) != 0)
        {
            free(event_data);
            event_list_free(events);
            return NULL;
        }

        // Move to next event
        offset += header->total_size;
    }

    return events;
}

// Write events to CSV file
int write_events_to_csv(const EventList_t *events, const char *output_filename)
{
    FILE *f = fopen(output_filename, "w");
    if (!f)
    {
        fprintf(stderr, "Error: Could not open output file '%s'\n", output_filename);
        return -1;
    }

    // Write CSV header
    fprintf(f, "event_number,board_id,pattern,channel_mask,event_counter,trigger_time_tag,channel,sample_index,value\n");

    // Write data
    for (size_t i = 0; i < events->count; i++)
    {
        const Event_t *event = &events->events[i];

        for (size_t j = 0; j < event->num_samples; j++)
        {
            fprintf(f, "%zu,%u,%u,%u,%u,%u,%u,%u,%u\n",
                    i,
                    event->board_id,
                    event->pattern,
                    event->channel_mask,
                    event->event_counter,
                    event->trigger_time_tag,
                    event->event_data[j].channel,
                    event->event_data[j].sample_index,
                    event->event_data[j].value_mv);
        }
    }

    fclose(f);
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

    // Read input file
    FILE *f = fopen(input_filename, "rb");
    if (!f)
    {
        fprintf(stderr, "Error: Could not open input file '%s'\n", input_filename);
        return 1;
    }

    // Get file size
    fseek(f, 0, SEEK_END);
    size_t file_size = ftell(f);
    fseek(f, 0, SEEK_SET);

    printf("Reading %zu bytes from '%s'...\n", file_size, input_filename);

    // Read entire file into buffer
    uint8_t *buffer = (uint8_t *)malloc(file_size);
    if (!buffer)
    {
        fprintf(stderr, "Error: Could not allocate memory\n");
        fclose(f);
        return 1;
    }

    size_t bytes_read = fread(buffer, 1, file_size, f);
    fclose(f);

    if (bytes_read != file_size)
    {
        fprintf(stderr, "Error: Could not read entire file\n");
        free(buffer);
        return 1;
    }

    // Parse buffer
    printf("Parsing buffer...\n");
    EventList_t *events = parse_acquisition_buffer(buffer, file_size);
    free(buffer);

    if (!events)
    {
        fprintf(stderr, "Error: Failed to parse buffer\n");
        return 1;
    }

    printf("Parsed %zu events\n", events->count);

    // Calculate total data points
    size_t total_data_points = 0;
    for (size_t i = 0; i < events->count; i++)
    {
        total_data_points += events->events[i].num_samples;
    }

    printf("Total data points: %zu\n", total_data_points);

    // Write to CSV
    printf("Writing to '%s'...\n", output_filename);
    if (write_events_to_csv(events, output_filename) != 0)
    {
        event_list_free(events);
        return 1;
    }

    printf("Success! CSV file written.\n");

    // Cleanup
    event_list_free(events);

    return 0;
}
