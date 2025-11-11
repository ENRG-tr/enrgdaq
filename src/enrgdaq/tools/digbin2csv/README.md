# ENRGDAQ tool: digbin2csv

digbin2csv is a tool that converts DAQJobCAENDigitizer's binary output into a CSV file.

# Build

Simply run:

```
make
```

Which in turn will create `digbin2csv` executable.

# Running

Simply run:

```
$ ./digbin2csv <input_file> [output_file]
  <input_file>  - Binary file containing acquisition data
  [output_file] - CSV file to write parsed data (optional, defaults to stdout)
```

Which in turn will create the transformed `output_file` at specified path, or print to stdout if no output file is provided.
