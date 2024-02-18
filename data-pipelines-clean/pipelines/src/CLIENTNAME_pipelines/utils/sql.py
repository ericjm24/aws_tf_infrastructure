import os


def read_sql(filename):
    with open(filename, "rt") as f:
        out = f.read()
    return out


def gather_sql(local_filename):
    report_templates = {}
    sql_path = os.path.dirname(os.path.realpath(local_filename))
    for file in os.listdir(sql_path):
        if file.endswith(".sql"):
            report = file.rsplit(".", 1)[0]
            report_templates[report] = read_sql(os.path.join(sql_path, file))
    return report_templates
