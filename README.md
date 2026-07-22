# AMAZON PRIME VIDEO LOG MONITORING SYSTEM USING BIG DATA ANALYTICS

## PROJECT OVERVIEW

This project simulates a real-time log monitoring system for Amazon Prime Video using Python. It creates three backend servers that continuously generate streaming-related logs. A multi-threaded Log Harvester collects these logs through TCP sockets, validates them using Regular Expressions (Regex), converts them into structured payloads, partitions them dynamically, and stores them as binary files. A Binary Log Reader is used to decode the stored binary logs back into a readable format.

---

## FEATURES

- Real-Time Log Monitoring
- Multi-threaded Log Harvesting
- TCP Socket Communication
- Custom Socket Buffer Slicing
- Regex-Based Log Validation
- Structured Payload Creation
- Dynamic Binary Partitioning
- Binary Log Storage
- Binary Log Reader
- Live Log Statistics

---

## TECHNOLOGIES USED

- Python 3
- Socket Programming
- Multithreading
- Regular Expressions (Regex)
- Struct Module
- Binary File Handling
- OS Module

---

## PROJECT STRUCTURE

```text
AmazonPrime_LogMonitoring/

│── log_server_simulator.py
│── log_harvester_daemon.py
│── read_binary_logs.py
│── README.md
│
└── prime_partitions/
```

---

## SIMULATED SERVERS

- Prime Login Server
- Prime Streaming Server
- Prime Content Server

Each server continuously generates streaming logs and sends them to the Log Harvester.

---

## PROJECT WORKFLOW

```text
Prime Backend Servers
        │
        ▼
TCP Socket Communication
        │
        ▼
Multi-threaded Log Harvester
        │
        ▼
Socket Stream Buffer Slicing
        │
        ▼
Regex Validation
        │
        ▼
Structured Payload Creation
        │
        ▼
Dynamic Binary Partitioning
        │
        ▼
Binary Storage (.bin)
        │
        ▼
Binary Log Reader
```

---

## HOW TO RUN

### Step 1: Start the Log Server

```bash
python log_server_simulator.py
```

### Step 2: Start the Log Harvester

Open another terminal and run:

```bash
python log_harvester_daemon.py
```

### Step 3: Read Stored Binary Logs

```bash
python read_binary_logs.py prime_partitions/prime-login-server_INFO.bin
```

Replace the filename with any available binary partition.

---

## SAMPLE OUTPUT

```text
2026-07-10 10:20:15 | INFO | prime-login-server | User logged in successfully

2026-07-10 10:20:19 | WARNING | prime-stream-server | Streaming quality reduced due to network fluctuation

2026-07-10 10:20:24 | ERROR | prime-content-server | Failed to load movie metadata
```

---

## PROJECT MODULES

### 1. log_server_simulator.py

- Simulates Amazon Prime backend servers
- Generates streaming-related logs
- Sends logs through TCP sockets

### 2. log_harvester_daemon.py

- Connects to all simulated servers
- Reads log streams in real time
- Performs socket slicing
- Validates logs using Regex
- Creates structured payloads
- Stores logs in binary partition files

### 3. read_binary_logs.py

- Reads binary partition files
- Decodes binary records
- Displays readable log information

---

## BIG DATA ANALYTICS CONCEPTS USED

- Real-Time Data Ingestion
- Stream Processing
- TCP Socket Communication
- Multi-threading
- Regular Expression Validation
- Dynamic Data Partitioning
- Binary File Storage
- Log Aggregation

---

## FUTURE ENHANCEMENTS

- Web Dashboard for Live Monitoring
- Cloud-Based Log Storage
- Database Integration
- Log Search and Filtering
- Email Alert System
- Performance Analytics
- AI-Based Anomaly Detection

---

AMAZON PRIME VIDEO STREAMING CATALOG ANALYSIS USING DISTRIBUTED MAPREDUCE ENGINE
WEEK – 2 | BIG DATA ANALYTICS PROJECT
PROJECT OVERVIEW

This project presents a custom Distributed MapReduce Engine developed from scratch using Python for analyzing an Amazon Prime Video-style streaming catalog.

The system is designed to study the growth and distribution of streaming content across different release years. Instead of performing a simple count based on one attribute, the project performs a two-dimensional analysis using Release Year and Content Type.

The system identifies how many Movies and TV Shows belong to each release year. This makes it possible to observe the distribution of content across different periods and understand how a streaming catalog is structured over time.

The MapReduce engine demonstrates a complete data-processing workflow involving:

Input data splitting
Parallel mapper execution
Intermediate key-value generation
Custom hash-based routing
Local disk-based partition management
Explicit sorting
Parallel reducer execution
Final aggregation
Year-wise content analysis

The project uses Python's multiprocessing module to execute mapper and reducer processes independently.

PROJECT OBJECTIVES

The major objectives of this project are:

To implement a MapReduce processing framework without using Hadoop or Spark.
To understand how large datasets can be divided into independent processing units.
To execute multiple mapper processes concurrently.
To extract meaningful analytical information from streaming content records.
To construct a composite MapReduce key using multiple dataset attributes.
To implement a custom hash partitioning mechanism.
To ensure that identical analytical keys reach the same reducer.
To maintain intermediate processing data using local disk files.
To perform an independent sorting stage before reduction.
To execute reducer processes concurrently.
To calculate year-wise Movie and TV Show distribution.
To demonstrate the practical use of distributed processing concepts in Big Data Analytics.
BUSINESS PROBLEM

Streaming platforms continuously expand their content libraries with movies and television shows released in different years.

For a streaming catalog, simply knowing the total number of available titles is not sufficient. It is also useful to understand:

Which years contributed the most content?
How many Movies were released in a particular year?
How many TV Shows were released in a particular year?
How does the content mix change across different years?

Processing these questions over a large catalog using a single sequential program can become inefficient.

This project addresses the problem by using a custom MapReduce architecture.

Each input record is transformed into a composite analytical key:

Release Year | Content Type

The corresponding value is:

1

For example:

2021|Movie → 1

The MapReduce engine then combines all records with the same composite key.

The final result answers questions such as:

2021 | Movie   | 5 Titles
2019 | TV Show | 3 Titles
2018 | Movie   | 4 Titles
ANALYTICAL APPROACH

The project uses two attributes as a composite key.

Input Record
Jai Bhim,Movie,2021,Drama
Extracted Information
Release Year = 2021
Content Type = Movie
Mapper Record
(2021|Movie, 1)

Multiple records with the same year and content type are aggregated together.

For example:

2021|Movie → 1
2021|Movie → 1
2021|Movie → 1
2021|Movie → 1
2021|Movie → 1

After reduction:

2021|Movie → 5

This approach allows the project to perform multi-dimensional content analytics instead of simply counting one category.

DATASET DESCRIPTION

The project uses a simulated Amazon Prime Video-style dataset.

Each record represents one streaming title.

Every record contains four attributes:

FIELD	DESCRIPTION
Title	Name of the movie or television show
Content Type	Movie or TV Show
Release Year	Year in which the title was released
Genre	Category of the content
DATA FORMAT
Title,Content Type,Release Year,Genre
SAMPLE DATA
The Family Man,TV Show,2019,Drama
Jai Bhim,Movie,2021,Drama
Soorarai Pottru,Movie,2020,Drama
Panchayat,TV Show,2020,Comedy
Sardar Ka Grandson,Movie,2021,Comedy
Suzhal,TV Show,2022,Crime
Mirzapur,TV Show,2018,Crime
Drishyam,Movie,2015,Thriller
Andhadhun,Movie,2018,Thriller
Breathe,TV Show,2018,Thriller
Dahaad,TV Show,2023,Crime
Raazi,Movie,2018,Drama
Shershaah,Movie,2021,Action
The Tomorrow War,Movie,2021,Action
Farzi,TV Show,2023,Crime
Made in Heaven,TV Show,2019,Drama
Tumbbad,Movie,2018,Horror
Parasyte,TV Show,2014,Horror
The Boys,TV Show,2019,Action
Pushpa,Movie,2021,Action
MAPREDUCE ENGINE DESIGN

The system consists of five major processing components:

                 AMAZON PRIME CATALOG
                         │
                         ▼
                    INPUT FILE
                         │
                         ▼
                  DATA CHUNKING
                         │
             ┌───────────┼───────────┐
             ▼           ▼           ▼
          MAPPER      MAPPER      MAPPER
             │           │           │
             └───────────┼───────────┘
                         ▼
                  COMPOSITE KEYS
              (YEAR | CONTENT TYPE, 1)
                         │
                         ▼
                  HASH ROUTING
                         │
               ┌─────────┴─────────┐
               ▼                   ▼
          DISK PARTITION 0    DISK PARTITION 1
               │                   │
               └─────────┬─────────┘
                         ▼
                   KEY SORTING
                         │
               ┌─────────┴─────────┐
               ▼                   ▼
            REDUCER 0           REDUCER 1
               │                   │
               └─────────┬─────────┘
                         ▼
                  FINAL ANALYSIS
                         │
                         ▼
                 OUTPUT REPORT
STAGE 1 – DATA CHUNKING

The complete dataset is first loaded from input.txt.

Instead of processing every record through one execution path, the input is divided into smaller chunks.

For example:

20 Input Records
       │
       ├── Chunk 1
       ├── Chunk 2
       ├── Chunk 3
       ├── Chunk 4
       ├── Chunk 5
       ├── Chunk 6
       └── Chunk 7

Each chunk is independently assigned to a mapper process.

This allows different portions of the dataset to be processed concurrently.

STAGE 2 – PARALLEL MAPPING

Each mapper reads its assigned records.

The mapper extracts:

Release Year
Content Type

The two values are combined to form a composite key.

Example

Input:

Pushpa,Movie,2021,Action

Mapper transformation:

(2021|Movie, 1)

Another record:

The Family Man,TV Show,2019,Drama

Becomes:

(2019|TV Show, 1)

The mapper does not perform aggregation.

Its responsibility is only to transform raw records into intermediate data.

STAGE 3 – HASH-BASED ROUTING

The intermediate records are distributed among reducer processes.

The routing mechanism uses:

hash(key) % number_of_reducers

The composite key is used for partitioning.

For example:

2021|Movie
2021|Movie
2021|Movie

All occurrences of this key are assigned to the same reducer.

This guarantees that a single reducer receives all values required to calculate the final total.

This is an important requirement of the MapReduce shuffle and partition stage.

STAGE 4 – DISK-BASED INTERMEDIATE STORAGE

Instead of directly passing all mapper results to reducers, the intermediate records are persisted locally.

The system creates:

intermediate/
│
├── partition_0.txt
└── partition_1.txt

Example partition content:

2021|Movie|1
2019|TV Show|1
2021|Movie|1
2018|Movie|1

This design provides a visible intermediate state that can be inspected during execution.

It also demonstrates how intermediate processing data can be stored before the reduction stage.

STAGE 5 – SORTING

The partition contents are sorted before the reducers begin processing them.

For example:

Before Sorting
2021|Movie|1
2018|Movie|1
2021|Movie|1
2019|TV Show|1
After Sorting
2018|Movie|1
2019|TV Show|1
2021|Movie|1
2021|Movie|1

The separate sorter.py module performs this operation.

Sorting makes records with identical keys easier to identify and group.

STAGE 6 – PARALLEL REDUCTION

Each reducer reads one partition file.

The reducer groups identical composite keys and collects their values.

Example:

2021|Movie
[1, 1, 1, 1, 1]

The reducer calculates:

1 + 1 + 1 + 1 + 1 = 5

The final reduced record becomes:

2021|Movie → 5

Multiple reducers execute independently on different partitions.

FINAL ANALYTICAL OUTPUT

The final output is presented in a readable format:

Year: 2014 | Content Type: TV Show | Total Titles: 1
Year: 2015 | Content Type: Movie | Total Titles: 1
Year: 2018 | Content Type: Movie | Total Titles: 4
Year: 2018 | Content Type: TV Show | Total Titles: 2
Year: 2019 | Content Type: TV Show | Total Titles: 3
Year: 2020 | Content Type: Movie | Total Titles: 1
Year: 2020 | Content Type: TV Show | Total Titles: 1
Year: 2021 | Content Type: Movie | Total Titles: 5
Year: 2022 | Content Type: TV Show | Total Titles: 1
Year: 2023 | Content Type: TV Show | Total Titles: 2

The result is saved to:

output/final_output.txt
PROJECT DIRECTORY STRUCTURE
Amazon_Prime_MapReduce/
│
├── main.py
├── mapper.py
├── partitioner.py
├── sorter.py
├── reducer.py
├── input.txt
├── README.md
│
├── intermediate/
│   ├── partition_0.txt
│   └── partition_1.txt
│
└── output/
    └── final_output.txt
MODULE RESPONSIBILITIES
main.py

Acts as the controller of the complete MapReduce pipeline.

It manages:

Input reading
Data chunking
Mapper process creation
Intermediate data collection
Partition creation
Sorting
Reducer process creation
Final output generation
mapper.py

Converts each raw streaming record into a composite key-value pair.

(Release Year|Content Type, 1)
partitioner.py

Determines the reducer responsible for each intermediate key.

hash(key) % number_of_reducers
sorter.py

Sorts each intermediate partition before the reducer phase.

reducer.py

Aggregates all values belonging to the same composite key.

input.txt

Contains the Amazon Prime Video-style catalog.

intermediate/

Contains temporary partitioned records generated during processing.

output/

Contains the final year-wise streaming analytics.

TECHNOLOGY USED
TECHNOLOGY	PURPOSE
Python 3	Core programming language
multiprocessing	Parallel mapper and reducer execution
MapReduce	Data processing model
Hash Function	Key-based partition routing
Local File System	Intermediate data persistence
Git	Version control
GitHub	Project repository
Visual Studio Code	Development environment
KEY FEATURES
COMPOSITE-KEY ANALYTICS

The project combines two attributes:

Release Year + Content Type

to perform multi-dimensional analysis.

PARALLEL MAPPING

Different input chunks are processed by independent mapper processes.

CUSTOM PARTITION ROUTING

Hash-based routing ensures that the same analytical key is processed by one reducer.

PERSISTENT INTERMEDIATE STATE

Mapper results are written to local partition files.

EXPLICIT SORTING STAGE

A dedicated sorting module prepares the intermediate records before reduction.

PARALLEL REDUCTION

Multiple reducer processes independently aggregate their assigned partitions.

STREAMING CATALOG INSIGHTS

The final output provides a time-based view of Movies and TV Shows in the catalog.

REQUIREMENT IMPLEMENTATION
ASSIGNMENT REQUIREMENT	PROJECT IMPLEMENTATION
Distributed MapReduce Engine	main.py
Input Splitting	Dataset divided into chunks
Independent Mappers	Python multiprocessing
Mapper Logic	mapper.py
Intermediate Key-Value Generation	(Year|Content Type, 1)
Custom Hash Partitioner	partitioner.py
Hash Partition Formula	hash(key) % reducers
Local Disk Storage	intermediate/
Sorting	sorter.py
Independent Reducers	Python multiprocessing
Aggregation	reducer.py
Final Output	output/final_output.txt
HOW TO EXECUTE
REQUIREMENT

Python 3.x must be installed.

Verify the installation:

python --version

Example:

Python 3.11.9
RUN THE PROJECT

Open the terminal inside the project directory:

cd Amazon_Prime_MapReduce

Execute:

python main.py

The engine automatically performs:

Read Dataset
    ↓
Create Input Chunks
    ↓
Launch Mapper Processes
    ↓
Generate Composite Key-Value Pairs
    ↓
Hash-Based Partitioning
    ↓
Write Intermediate Files
    ↓
Sort Partition Files
    ↓
Launch Reducer Processes
    ↓
Aggregate Results
    ↓
Generate Final Report
OUTPUT LOCATION

Final results:

output/final_output.txt

Intermediate results:

intermediate/partition_0.txt
intermediate/partition_1.txt
SAMPLE EXECUTION
=================================================================
AMAZON PRIME VIDEO YEAR-WISE MAPREDUCE ANALYTICS ENGINE
=================================================================

Input Split Completed
Number of Mapper Chunks: 7

Mapper Processes Completed

Hash Partitioning Completed
Sorting Completed
Reducer Processes Completed

=================================================================
FINAL AMAZON PRIME VIDEO YEAR-WISE CONTENT ANALYSIS
=================================================================

Year: 2014 | Content Type: TV Show | Total Titles: 1
Year: 2015 | Content Type: Movie | Total Titles: 1
Year: 2018 | Content Type: Movie | Total Titles: 4
Year: 2018 | Content Type: TV Show | Total Titles: 2
Year: 2019 | Content Type: TV Show | Total Titles: 3
Year: 2020 | Content Type: Movie | Total Titles: 1
Year: 2020 | Content Type: TV Show | Total Titles: 1
Year: 2021 | Content Type: Movie | Total Titles: 5
Year: 2022 | Content Type: TV Show | Total Titles: 1
Year: 2023 | Content Type: TV Show | Total Titles: 2

Final Output Saved To:
output/final_output.txt

MapReduce Job Completed Successfully!
KEY FINDING

Based on the sample dataset, 2021 contains the highest number of Movie titles, with a total of 5 titles.

This demonstrates how the MapReduce engine can transform raw streaming catalog records into useful time-based analytical information.

LEARNING OUTCOMES

This project provides practical understanding of:

MapReduce architecture
Parallel processing
Process-based concurrency
Data chunking
Mapper execution
Composite keys
Hash-based partitioning
Intermediate data persistence
External sorting concepts
Reducer execution
Key-based aggregation
Distributed computing principles
Big Data processing workflows
Modular Python development
FUTURE ENHANCEMENTS

The project can be extended to support:

Larger streaming catalogs
Dynamic mapper and reducer configuration
Year and genre cross-analysis
Country-wise content distribution
Movie versus TV Show trends
Content growth visualization
Rating-based analytics
Audience popularity analysis
Interactive dashboards
Real-world streaming datasets
API-based data collection
Hadoop integration
Apache Spark integration
Multi-machine distributed execution
CONCLUSION

The Amazon Prime Video Streaming Catalog Analysis Using Distributed MapReduce Engine project demonstrates how a custom MapReduce framework can be used to analyze structured streaming content data.

The project goes beyond basic counting by creating a composite analytical key from Release Year and Content Type. This enables the system to identify the distribution of Movies and TV Shows across different release years.

The implementation demonstrates the complete processing lifecycle, including parallel input processing, mapper execution, hash-based routing, intermediate disk storage, explicit sorting, and parallel reduction.

The project provides a practical understanding of how distributed processing techniques can transform a large collection of raw records into meaningful analytical results.

Overall, the project establishes a strong foundation in MapReduce, parallel computing, partitioning, sorting, aggregation, and Big Data analytics, while demonstrating a practical streaming-platform use case.






## AUTHOR

K.BRAJESH SHEGUWARE
II-BCA-'B'

---
