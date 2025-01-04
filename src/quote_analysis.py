import polars as pl
import os
import argparse
from datetime import datetime,timezone


pl.Config.set_tbl_rows(1000)



def cla_parser_setup():

    parser = argparse.ArgumentParser(
                description='parse command line arguments for cmc quote analysis',
            )

    parser.add_argument(
        "--reference_symbol",
        type = str,
        default = "BTC",
        required = False,
        help = "Crypto symbol to calculate relative 24hr percent change differences against"
    )

    return parser


def read_csv_files(path: str):

    """
    read in and concat batch quote csv files into polars DataFrame
    NOTE: breaks if schemas of all batch files are not the same (namely as a result of changing coins_to_track.csv symbol list)

    parameters:
    path (str): directory to read files from

    returns:
    pl.DataFrame
    """

    files = os.listdir(path)

    df = pl.concat([pl.read_csv(f"{path}/{filename}").select("LoadedWhen","last_updated","symbol","percent_change_24h") for filename in sorted(files)[::-1]])
    
    return df

def add_reference_symbol_fields(df: pl.DataFrame, symbol: str, join_fields: dict ,target_field: str) -> pl.DataFrame:

    """

    generate new field w/ numerical value of a target field (ex. percent_change_24h) for a reference symbol (ex. BTC) over a join field (ex. LoadedWhen)
    used later to make relative calculations.


    parameters:

    df (polars.DataFrame): source dataframe to left join against
    symbol (str): list of symbols to filter source df by
    join_fields (dict): field map to join on {"left":"LoadedWhen", "right":"LoadedWhen"}
    target_field (str): the target numerical field we want to use for generating new reference fields

    returns: pl.DataFrame
    """


    reference_records = df.filter(pl.col("symbol") == symbol)

    if reference_records.shape != (0,0):

        df = df.join(
            reference_records.select(join_fields["right"], target_field), 
            how = "left",
            left_on = join_fields["left"],
            right_on = join_fields["right"],
            suffix = f"_{symbol}"
        )

    return df


if __name__ == "__main__":

    CLA = cla_parser_setup().parse_args()

    ts = datetime.now(timezone.utc)

    if os.getcwd().endswith("src"):

        # create landing directories for data if they don't exist already
        for dir_ in ["./analysis","./analysis/diffs","./analysis/average_diffs"]:
            try:
                os.mkdir(dir_)
            except FileExistsError:
                continue

    else:
        
        raise Exception("\n\n******Please run this script from within the src/ directory!******\n\n")


    # read in all price quote files
    df = read_csv_files(path = "./extracts/quotes")

    symbol = CLA.reference_symbol

    df = add_reference_symbol_fields(df = df, symbol = symbol, join_fields = {"left":"LoadedWhen", "right":"LoadedWhen"}, target_field = "percent_change_24h")

    # calculate relative percent change vs BTC
    df = df.with_columns(
            (pl.col("percent_change_24h") - pl.col(f"percent_change_24h_{symbol}")).alias(f"relative_percent_change_24h_vs_{symbol}")
        )

    df.write_csv(f"analysis/diffs/{ts.strftime('%Y%m%dT%H%M%S')}_relative_24h_percent_change_vs_{symbol}.csv")

    df = df.select(
            "symbol",
            f"relative_percent_change_24h_vs_{symbol}"
        ).group_by("symbol").agg(pl.mean(f"relative_percent_change_24h_vs_{symbol}").name.prefix("avg_"))


    # Might be useful but could cause problems because this value will be a growing string with each run as
    # averages are taken over more and more files    
    # extracts_to_analyze = "|".join(os.listdir("./extracts/quotes"))
    # df = df.with_columns(
    #     pl.lit(extracts_to_analyze).alias("SourceFileSet")
    # )

    df.write_csv(f"analysis/average_diffs/{ts.strftime('%Y%m%dT%H%M%S')}_avg_relative_24h_percent_change_vs_{symbol}.csv")

    # print(df.sort(f"avg_relative_percent_change_24h_vs_{symbol}", descending = True))

