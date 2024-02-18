from io import StringIO
import csv


def parse_s3_csv(input, *args, **kwargs):
    with StringIO(input) as f:
        reader = csv.reader(f, *args, **kwargs)
        out = [row for row in reader]
    return out


def csv_to_string(input, **kwargs):
    with StringIO() as f:
        writer = csv.writer(f, **kwargs)
        writer.writerows(input)
        out = f.getvalue()
    return out
