"""
get data from csv file and clean it via cmd line argument and write to new csv file supplied via cmd line argument as well. Also write to an sqlite db and move it to /home/metabase-data
"""

import pandas as pd
import sys

csv_file = sys.argv[1]
new_csv_file = sys.argv[2]


METABASE_DB_FILE_PATH = "sqlite:////home/redgren/metabase-data/acn_clean.db"

LAST_COURSE_ACTIVITY_COL = "Last Course Activity"


def clean_data(csv_file) -> pd.DataFrame:
    df = pd.read_csv(csv_file)
    date_columns = [
        LAST_COURSE_ACTIVITY_COL,
        "Latest Data extraction date",
        "Collection completed date",
        "Target End Date (from Group)",
    ]

    df[date_columns] = df[date_columns].apply(pd.to_datetime, errors="coerce")

    df["Activity"] = "No Activity"
    df.loc[
        (df[LAST_COURSE_ACTIVITY_COL] >= pd.Timestamp("today") - pd.Timedelta(days=7)),
        "Activity",
    ] = "Active This Week"
    df.loc[
        (df[LAST_COURSE_ACTIVITY_COL] >= pd.Timestamp("today") - pd.Timedelta(days=14))
        & (df[LAST_COURSE_ACTIVITY_COL] < pd.Timestamp("today") - pd.Timedelta(days=7)),
        "Activity",
    ] = "Last Week"
    df.loc[
        (df[LAST_COURSE_ACTIVITY_COL] >= pd.Timestamp("today") - pd.Timedelta(days=21))
        & (
            df[LAST_COURSE_ACTIVITY_COL] < pd.Timestamp("today") - pd.Timedelta(days=14)
        ),
        "Activity",
    ] = "Last 2 Weeks"
    df.loc[
        (df[LAST_COURSE_ACTIVITY_COL] >= pd.Timestamp("today") - pd.Timedelta(days=28))
        & (
            df[LAST_COURSE_ACTIVITY_COL] < pd.Timestamp("today") - pd.Timedelta(days=21)
        ),
        "Activity",
    ] = "Last 3 Weeks"
    df.loc[
        (df[LAST_COURSE_ACTIVITY_COL] >= pd.Timestamp("today") - pd.Timedelta(days=30))
        & (
            df[LAST_COURSE_ACTIVITY_COL] < pd.Timestamp("today") - pd.Timedelta(days=28)
        ),
        "Activity",
    ] = "No Activity 1 Month"
    df.loc[
        df[LAST_COURSE_ACTIVITY_COL] < pd.Timestamp("today") - pd.Timedelta(days=30),
        "Activity",
    ] = "No Activity"
    return df


if __name__ == "__main__":
    clean_df = clean_data(csv_file)
    clean_df.to_csv(new_csv_file, index=False)
    clean_df.to_sql("clean_data", METABASE_DB_FILE_PATH, if_exists="replace")

    print(
        f"""
        Connection details for Metabase:
        Database type: SQLite
        Database name: metabase-data/acn_clean.db
    """
    )
