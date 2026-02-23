def model(dbt, session):

    dbt.config(
        materialized="table"
    )

    # dbt dependency reference
    df = dbt.ref("customer")

    result_df = df.select(
        "c_custkey",
        "c_name",
        "c_nationkey"
    )

    return result_df