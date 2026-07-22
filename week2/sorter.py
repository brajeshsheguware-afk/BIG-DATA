def sort_partition(filename):

    with open(filename, "r") as file:

        records = file.readlines()


    records.sort(
        key=lambda line: line.split("|")[0]
    )


    with open(filename, "w") as file:

        for record in records:

            file.write(record)