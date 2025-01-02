import requests
import polars as pl
import json
import os
from datetime import datetime,timezone
from dotenv import load_dotenv



def cmc_extract(cmc_endpoint_url: str, headers: dict, parameters: dict) -> dict:

    """
    returns: dict api response 
    """

    session = requests.Session()
    session.headers.update(headers)

    responses = json.loads(session.get(cmc_endpoint_url, params = parameters).text)

    return responses



def get_quotes(cmc_endpoint_url: str, headers: dict, parameters: dict, csv_write_path: str) -> None:

    responses = cmc_extract(cmc_endpoint_url = cmc_endpoint_url, headers = headers, parameters = parameters)
    
    ts = datetime.now(timezone.utc)

    returned_data = responses["data"]
    returned_symbols = list(returned_data.keys())

    # list comp to build list of LazyFrames (Lazy evaluated DataFrame) with quote data for each symbol
    # and adding symbol identifier, IsTopCurrency, and LoadedWhen columns to each chunk
    quote_lfs = [
        pl.LazyFrame(data["quote"]["USD"]).with_columns(
                pl.lit(symbol).alias("symbol"),
                pl.lit(int(returned_data[symbol]["cmc_rank"])<=10).alias("IsTopCurrency"),
                pl.lit(ts.strftime("%Y-%m-%dT%H:%M:%SZ")).alias("LoadedWhen")
            ) 
        for symbol,data in returned_data.items()
    ]

    # concat quote dfs to one single LazyFrame
    lf = pl.concat(quote_lfs, how = 'vertical_relaxed')

    # materialize to DataFrame w/ collect() and write data to local path as csv
    lf.collect().write_csv(file = fr"{csv_write_path}/{ts.strftime('%Y%m%dT%H%M%S')}_quotes.csv")

    return lf



def get_metadata(cmc_endpoint_url: str, headers: dict, parameters: dict, csv_write_path: str) -> None:

    responses = cmc_extract(cmc_endpoint_url = cmc_endpoint_url, headers = headers, parameters = parameters)
    
    ts = datetime.now(timezone.utc)

    returned_data = responses["data"]
    returned_symbols = list(returned_data.keys())

    meta_dfs = [pl.from_records(data) for symbol,data in returned_data.items()]
    meta_df = pl.concat([df for df in meta_dfs if df.shape != (0,0)], how = "vertical_relaxed")


    # Unnesting contract_address, platform, and urls nested fields
    meta_df = \
        meta_df.with_columns(

            pl.col("contract_address").list.first().struct.rename_fields(["contract_address","contract_address.platform"])

        ).unnest("contract_address").with_columns(

            pl.col("contract_address.platform").struct.rename_fields(["contract_address.platform.name","contract_address.platform.coin"])

        ).unnest("contract_address.platform").with_columns(

            pl.col("contract_address.platform.coin").struct.rename_fields(["contract_address.platform.coin.id","contract_address.platform.coin.name","contract_address.platform.coin.symbol","contract_address.platform.coin.slug"])

        ).unnest("contract_address.platform.coin").with_columns(

            pl.col("platform").struct.rename_fields(["platform.id","platform.name","platform.slug","platform.symbol","platform.token_address"])

        ).unnest(["platform","urls"])

    
    meta_df = meta_df.with_columns(pl.lit(ts.strftime("%Y-%m-%dT%H:%M:%SZ")).alias("LoadedWhen"))


    for column,type in dict(zip(meta_df.columns,meta_df.dtypes)).items():
        # Convert fields of type List[str] to pipe delimited strings so we can write to csv
        if type == pl.List(str):
            meta_df = meta_df.with_columns(pl.col(column).list.join("|"))
        elif type == pl.List(pl.Null):
            # convert fields of type List[Null] to Null
            meta_df = meta_df.with_columns(pl.when(pl.col(column).list == []).then(pl.col(column)))
            meta_df = meta_df.with_columns(pl.col(column).cast(pl.Null))


    meta_df.write_csv(file = fr"{csv_write_path}/{ts.strftime('%Y%m%dT%H%M%S')}_metadata.csv")

    return meta_df.lazy()



def get_map(cmc_endpoint_url: str, headers: dict, parameters: dict, csv_write_path: str) -> None:

    responses = cmc_extract(cmc_endpoint_url = cmc_endpoint_url, headers = headers, parameters = parameters)

    ts = datetime.now(timezone.utc)

    returned_data = responses["data"]

    # and adding IsTopCurrency, LoadedWhen columns and unnesting platform
    map_df = pl.from_records(returned_data).with_columns(

            (pl.col("rank").cast(pl.Int64) <= 10).alias("IsTopCurrency"),

            # rename keys in nested field before unnesting
            pl.col("platform").struct.rename_fields(["platform.id","platform.name","platform.slug","platform.symbol","platform.token_address"]),

            pl.lit(ts.strftime("%Y-%m-%dT%H:%M:%SZ")).alias("LoadedWhen")

        ).unnest("platform")

    map_df.write_csv(file = fr"{csv_write_path}/{ts.strftime('%Y%m%dT%H%M%S')}_map.csv")

    return map_df.lazy()

def main():

    coins_to_track = pl.read_csv("../coins_to_track.csv")
    symbols = ','.join(coins_to_track['Symbol'])
    
    load_dotenv()
    cmc_key = os.getenv("X-CMC_PRO_API_KEY")

    headers = {
        "X-CMC_PRO_API_KEY": cmc_key,
        "Content-Type": "application/json"
    }

    quotes = get_quotes(        
        cmc_endpoint_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest", 
        headers = headers, 
        parameters = {"symbol":symbols, "skip_invalid": "true"}, 
        csv_write_path = "extracts/quotes"
    )

    map_ = get_map(
        cmc_endpoint_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map", 
        headers = headers, 
        parameters = {}, 
        csv_write_path = "extracts/map"
    )

    metadata = get_metadata(
        cmc_endpoint_url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/info", 
        headers = headers, 
        parameters = {"symbol":symbols, "skip_invalid": "true"}, 
        csv_write_path = "extracts/metadata"
    )

if __name__ == "__main__":
    
    main()
