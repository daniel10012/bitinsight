import CONFIG


def bitcoin_files_names(n):
    file_names = []
    n = int(n)
    for i in range(n, n+10000):
        file_name = f"{CONFIG.S3blocks}/block{i}.json"
        file_names.append(file_name)
    return file_names
