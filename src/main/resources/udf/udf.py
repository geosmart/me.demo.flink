from pyflink.table import DataTypes
from pyflink.table.udf import udf


@udf(
    input_types=[
        DataTypes.STRING(), DataTypes.STRING(), DataTypes.BIGINT()
    ],
    result_type=DataTypes.STRING()
)
def metric_udf(user, url, timestamp):
    # function code here
    return f"{user}|{url}|{timestamp}"


def test_pands():
    import pandas as pd
    d = {'col1': [1, 2], 'col2': [3, 4]}
    df = pd.DataFrame(data=d)
    df.head(10)
