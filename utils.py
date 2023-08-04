def split_into_groups(lst, n) -> list:
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
