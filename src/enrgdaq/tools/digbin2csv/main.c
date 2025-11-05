#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#define HEADER_SIZE 6

int main(int argc, char *argv[])
{
    if (argc != 3)
    {
        fprintf(stderr, "Usage: %s <input_file> <output_file>\n", argv[0]);
        return 1;
    }

    char *input_filename = argv[1];
    char *output_filename = argv[2];

    FILE *input_file = fopen(input_filename, "rb");
    if (!input_file)
    {
        perror("Error opening input file");
        return 1;
    }

    FILE *output_file = fopen(output_filename, "w");
    if (!output_file)
    {
        perror("Error opening output file");
        fclose(input_file);
        return 1;
    }

    fprintf(output_file, "Timestamp,Channel,Value\n");

    uint32_t header_data[HEADER_SIZE];

    while (fread(header_data, sizeof(uint32_t), HEADER_SIZE, input_file) == HEADER_SIZE)
    {
        uint32_t data_len_u16 = header_data[0];

        if (data_len_u16 % 3 != 0)
        {
            fprintf(stderr, "Error: Data length %u is not a multiple of 3.\n", data_len_u16);
            continue;
        }

        uint32_t num_tuples = data_len_u16 / 3;
        uint32_t timestamp_base = header_data[5];

        for (uint32_t i = 0; i < num_tuples; ++i)
        {
            uint16_t waveform_data[3];
            if (fread(waveform_data, sizeof(uint16_t), 3, input_file) != 3)
            {
                fprintf(stderr, "Error: Incomplete waveform data tuple.\n");
                goto end_of_file;
            }
            uint16_t ch = waveform_data[0];
            uint16_t sample_index = waveform_data[1];
            uint16_t value = waveform_data[2];
            uint64_t timestamp = (uint64_t)timestamp_base + sample_index;

            fprintf(output_file, "%llu,%u,%u\n", timestamp, ch, value);
        }
    }

end_of_file:
    fclose(input_file);
    fclose(output_file);

    printf("Conversion complete.\n");

    return 0;
}
