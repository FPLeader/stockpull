def yield_rows(cursor, chunk_size):
    chunk = []
    for i, row in enumerate(cursor):
        if i % chunk_size == 0 and i > 0:
            yield chunk; del chunk[:]
        chunk.append(row)
    yield chunk