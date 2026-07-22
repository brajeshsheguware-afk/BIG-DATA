import multiprocessing
import os

from mapper import mapper
from partitioner import partition
from sorter import sort_partition
from reducer import reducer


NUMBER_OF_REDUCERS = 2
CHUNK_SIZE = 3


# ==========================================
# MAPPER WORKER
# ==========================================

def mapper_worker(lines):

    result = []

    for line in lines:

        mapped_values = mapper(line)

        result.extend(mapped_values)

    return result


# ==========================================
# CREATE PARTITIONS
# ==========================================

def create_partitions(mapped_data):

    os.makedirs(
        "intermediate",
        exist_ok=True
    )


    partitions = {

        i: []

        for i in range(
            NUMBER_OF_REDUCERS
        )

    }


    for key, value in mapped_data:

        reducer_id = partition(
            key,
            NUMBER_OF_REDUCERS
        )


        partitions[
            reducer_id
        ].append(
            (key, value)
        )


    for reducer_id in range(
        NUMBER_OF_REDUCERS
    ):

        filename = (

            "intermediate/"
            f"partition_{reducer_id}.txt"

        )


        with open(
            filename,
            "w"
        ) as file:

            for key, value in partitions[
                reducer_id
            ]:

                file.write(

                    f"{key}|{value}\n"

                )


# ==========================================
# REDUCER WORKER
# ==========================================

def reducer_worker(reducer_id):

    filename = (

        "intermediate/"
        f"partition_{reducer_id}.txt"

    )


    grouped = {}


    if not os.path.exists(
        filename
    ):

        return []


    with open(
        filename,
        "r"
    ) as file:

        for line in file:

            line = line.strip()


            if not line:

                continue


            key, value = line.rsplit(
                "|",
                1
            )


            value = int(
                value
            )


            if key not in grouped:

                grouped[key] = []


            grouped[
                key
            ].append(
                value
            )


    output = []


    for key in sorted(
        grouped
    ):

        output.append(

            reducer(

                key,

                grouped[key]

            )

        )


    return output


# ==========================================
# MAIN PROGRAM
# ==========================================

if __name__ == "__main__":


    print("=" * 65)

    print(
        "AMAZON PRIME VIDEO "
        "YEAR-WISE MAPREDUCE ANALYTICS ENGINE"
    )

    print("=" * 65)


    # ======================================
    # READ INPUT
    # ======================================

    with open(
        "input.txt",
        "r"
    ) as file:

        lines = file.readlines()


    # ======================================
    # INPUT SPLITTING
    # ======================================

    chunks = []


    for i in range(
        0,
        len(lines),
        CHUNK_SIZE
    ):

        chunks.append(

            lines[
                i:i + CHUNK_SIZE
            ]

        )


    print(
        "\nInput Split Completed"
    )


    print(

        "Number of Mapper Chunks:",

        len(chunks)

    )


    # ======================================
    # PARALLEL MAPPER PROCESSES
    # ======================================

    mapper_pool = multiprocessing.Pool(

        processes=len(chunks)

    )


    mapper_results = mapper_pool.map(

        mapper_worker,

        chunks

    )


    mapper_pool.close()

    mapper_pool.join()


    print(

        "\nMapper Processes Completed"

    )


    # ======================================
    # COMBINE MAPPER OUTPUT
    # ======================================

    intermediate = []


    for result in mapper_results:

        intermediate.extend(

            result

        )


    print(

        "\nIntermediate Key-Value Pairs:"

    )


    for key, value in intermediate:

        print(

            f"({key}, {value})"

        )


    # ======================================
    # HASH PARTITIONING
    # ======================================

    create_partitions(

        intermediate

    )


    print(

        "\nHash Partitioning Completed"

    )


    # ======================================
    # SORT PARTITIONS
    # ======================================

    for reducer_id in range(

        NUMBER_OF_REDUCERS

    ):

        filename = (

            "intermediate/"

            f"partition_{reducer_id}.txt"

        )


        sort_partition(

            filename

        )


    print(

        "Sorting Completed"

    )


    # ======================================
    # PARALLEL REDUCER PROCESSES
    # ======================================

    reducer_pool = multiprocessing.Pool(

        processes=NUMBER_OF_REDUCERS

    )


    reducer_results = reducer_pool.map(

        reducer_worker,

        range(
            NUMBER_OF_REDUCERS
        )

    )


    reducer_pool.close()

    reducer_pool.join()


    print(

        "Reducer Processes Completed"

    )


    # ======================================
    # COMBINE REDUCER OUTPUT
    # ======================================

    final_results = []


    for result in reducer_results:

        final_results.extend(

            result

        )


    # Sort by year and content type

    final_results.sort(

        key=lambda item: (

            item[0].split("|")[0],

            item[0].split("|")[1]

        )

    )


    # ======================================
    # CREATE OUTPUT DIRECTORY
    # ======================================

    os.makedirs(

        "output",

        exist_ok=True

    )


    output_file = (

        "output/"

        "final_output.txt"

    )


    # ======================================
    # SAVE FINAL OUTPUT
    # ======================================

    with open(

        output_file,

        "w"

    ) as file:


        for key, total in final_results:

            year, content_type = key.split(

                "|"

            )


            file.write(

                f"Year: {year} | "

                f"Content Type: {content_type} | "

                f"Total Titles: {total}\n"

            )


    # ======================================
    # DISPLAY FINAL OUTPUT
    # ======================================

    print(

        "\n" + "=" * 65

    )


    print(

        "FINAL AMAZON PRIME VIDEO "
        "YEAR-WISE CONTENT ANALYSIS"

    )


    print(

        "=" * 65

    )


    for key, total in final_results:


        year, content_type = key.split(

            "|"

        )


        print(

            f"Year: {year} | "

            f"Content Type: {content_type} | "

            f"Total Titles: {total}"

        )


    print(

        "\nFinal Output Saved To:"

    )


    print(

        output_file

    )


    print(

        "\nMapReduce Job Completed Successfully!"

    )