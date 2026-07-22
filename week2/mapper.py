def mapper(line):

    parts = line.strip().split(",")

    if len(parts) != 4:
        return []

    title = parts[0]
    content_type = parts[1]
    release_year = parts[2]
    genre = parts[3]

    key = f"{release_year}|{content_type}"

    return [(key, 1)]