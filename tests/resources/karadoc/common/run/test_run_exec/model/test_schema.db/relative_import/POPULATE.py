from karadoc.common.run import Job

from .dep2.sub_dep import value

job = Job()


def run():
    return job.spark.sql(f"""SELECT '{value}' as value""")
