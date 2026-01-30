import os
import warnings
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv

# Suppress Python deprecation warnings from boto3
warnings.filterwarnings("ignore", category=DeprecationWarning, module="boto3")

import boto3
import pandas as pd
from supabase import create_client, Client  # type: ignore

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))




DDB_TABLE_NAME = os.getenv("DDB_TABLE_NAME", "shop_daily_unique_customers")




def get_supabase_client() -> Client:
    """
    Create and return a Supabase client using environment variables.


    Required environment variables:
    - SUPABASE_URL
    - SUPABASE_SERVICE_ROLE_KEY or SUPABASE_ANON_KEY
    """
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")


    if not url or not key:
        raise RuntimeError(
            "Supabase credentials not set. "
            "Please define SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY or SUPABASE_ANON_KEY."
        )


    return create_client(url, key)




def fetch_table_rows(
    table_name: str,
    limit: Optional[int] = None,
    filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch rows from a Supabase table.


    - table_name: name of the table in your Supabase database.
    - limit: optional max number of rows to return.
    - filters: optional dict of column -> value equality filters.
    """
    supabase = get_supabase_client()
    query = supabase.table(table_name).select("*")


    if filters:
        for column, value in filters.items():
            query = query.eq(column, value)


    if limit is not None:
        query = query.limit(limit)


    response = query.execute()
    return response.data or []




def fetch_table_df(
    table_name: str,
    limit: Optional[int] = None,
    filters: Optional[Dict[str, Any]] = None,
) -> "pd.DataFrame":
    """
    Fetch rows from a Supabase table and return them as a Pandas DataFrame.
    """
    rows = fetch_table_rows(table_name=table_name, limit=limit, filters=filters)
    return pd.DataFrame(rows)




def save_unique_customer_count_ddb(
    shop_id: str,
    date_str: str,
    unique_customers: int,
    table_name: str = DDB_TABLE_NAME,
) -> None:
    """
    Save (upsert) the unique customer count for a shop and date into DynamoDB.
    Partition key: shop_id (string)
    Sort key: date (string, e.g. '2026-01-21')
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)


    table.put_item(
        Item={
            "shop_id": shop_id,
            "date": date_str,
            "unique_customers": unique_customers,
            "updated_at": datetime.now(tz=timezone.utc).isoformat(),
        }
    )




if __name__ == "__main__":
    # 1) Get today's data from Supabase (with early-hour adjustment)
    df = fetch_table_df("terminal_logs")


    if df.empty:
        print("No data returned from Supabase.")
    else:
        # Ensure timestamp column is parsed; adjust 'created_at' if your column name differs
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True)


        now_utc = datetime.now(tz=timezone.utc)
        # If between 00:00 and 00:59 UTC, use previous day; otherwise use current day
        if now_utc.hour == 0:
            target_date = (now_utc - timedelta(days=1)).date()
            print(f"[INFO] Computing for YESTERDAY ({target_date.isoformat()}) because current UTC hour is {now_utc.hour}.")
        else:
            target_date = now_utc.date()
            print(f"[INFO] Computing for TODAY ({target_date.isoformat()}) with current UTC hour {now_utc.hour}.")


        df_today = df[df["created_at"].dt.date == target_date]


        if df_today.empty:
            print(f"No data for target date ({target_date.isoformat()}).")
        else:
            # Get all unique shop_id for today
            unique_shop_ids = df_today["shop_id"].dropna().unique()


            print(f"Processing {len(unique_shop_ids)} shops for {target_date.isoformat()}")


            # Loop through each shop_id
            for shop_id in unique_shop_ids:
                df_shop = df_today[df_today["shop_id"] == shop_id]


                # Each unique phone_number counts as 1 customer
                unique_customers = df_shop["phone_number"].dropna().astype(str).nunique()


                # Save to DynamoDB after each loop
                save_unique_customer_count_ddb(
                    shop_id=str(shop_id),
                    date_str=target_date.isoformat(),
                    unique_customers=int(unique_customers),
                )


                print(f"Shop {shop_id}: date={target_date.isoformat()}, unique_customers={unique_customers}")





