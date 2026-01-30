import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from supabase import create_client
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

def get_supabase_client():
    """Create and return a Supabase client using environment variables."""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")

    if not url or not key:
        raise RuntimeError(
            "Supabase credentials not set. "
            "Please define SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY or SUPABASE_ANON_KEY."
        )

    return create_client(url, key)

def generate_test_data(
    num_shops: int = 5,
    num_logs_per_shop: int = 20,
    days_back: int = 0,
    table_name: str = "terminal_logs",
    shop_id: str = None
):
    """
    Generate test data in Supabase terminal_logs table.
    
    Args:
        num_shops: Number of unique shop_ids to generate (ignored if shop_id is provided)
        num_logs_per_shop: Number of log entries per shop per day
        days_back: How many days back to generate data (0 = today only, 1 = today + yesterday, etc.)
        table_name: Name of the Supabase table
        shop_id: Specific shop_id to use (if provided, num_shops is ignored and only this shop_id is used)
    """
    supabase = get_supabase_client()
    
    # Use provided shop_id or generate shop IDs as UUIDs (Supabase expects UUID format)
    if shop_id:
        shop_ids = [shop_id]
    else:
        shop_ids = [str(uuid.uuid4()) for _ in range(num_shops)]
    
    # Generate phone numbers (Singapore format: +65XXXXXXXX)
    phone_numbers = [f"+65{random.randint(80000000, 99999999)}" for _ in range(50)]
    
    # Generate data for specified days (days_back = 0 means today only)
    now = datetime.now(tz=timezone.utc)
    all_data = []
    
    for day_offset in range(days_back + 1):  # +1 to include today
        target_date = (now - timedelta(days=day_offset)).date()
        
        for shop_id in shop_ids:
            # Generate random number of logs for this shop on this day
            num_logs = random.randint(5, num_logs_per_shop)
            
            for _ in range(num_logs):
                # Random time within the day
                hour = random.randint(0, 23)
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                
                created_at = datetime.combine(
                    target_date,
                    datetime.min.time()
                ).replace(hour=hour, minute=minute, second=second, tzinfo=timezone.utc)
                
                # Random phone number
                phone_number = random.choice(phone_numbers)
                
                all_data.append({
                    "shop_id": shop_id,
                    "phone_number": phone_number,
                    "transaction_type": "stamp_collection",  # Required field - using valid value
                    "created_at": created_at.isoformat()
                })
    
    # Insert data in batches (Supabase has limits)
    batch_size = 100
    total_inserted = 0
    
    print(f"Generating {len(all_data)} log entries for {num_shops} shops across {days_back} days...")
    
    if len(all_data) == 0:
        print("⚠️  WARNING: No data generated! Check your parameters.")
        return
    
    # Show sample of first record
    print(f"\nSample record: {all_data[0]}\n")
    
    for i in range(0, len(all_data), batch_size):
        batch = all_data[i:i + batch_size]
        try:
            result = supabase.table(table_name).insert(batch).execute()
            # Check if insertion was successful
            if hasattr(result, 'data') and result.data:
                inserted_count = len(result.data)
                total_inserted += inserted_count
                print(f"✅ Inserted batch {i//batch_size + 1}: {inserted_count} records (Total: {total_inserted}/{len(all_data)})")
            elif hasattr(result, 'data') and result.data is None:
                # Sometimes Supabase returns None but insertion succeeded
                total_inserted += len(batch)
                print(f"✅ Inserted batch {i//batch_size + 1}: {len(batch)} records (Total: {total_inserted}/{len(all_data)})")
            else:
                print(f"⚠️  Warning: Batch {i//batch_size + 1} - No data in response. Full response: {result}")
        except Exception as e:
            print(f"❌ Error inserting batch {i//batch_size + 1}: {type(e).__name__}: {e}")
            # Print first record of batch for debugging
            if batch:
                print(f"   Sample record from failed batch: {batch[0]}")
            import traceback
            print(traceback.format_exc())
            # Continue with next batch
    
    print(f"\n✅ Successfully generated {total_inserted} log entries!")
    print(f"\nShop IDs generated: {', '.join(shop_ids)}")
    print(f"\nYou can now run analytics.py to process this data.")

if __name__ == "__main__":
    import sys
    
    # Parse command line arguments (optional)
    # Usage: python3 generate_test_data.py [num_shops] [num_logs_per_shop] [days_back] [shop_id]
    # Or: python3 generate_test_data.py [shop_id] (if shop_id is provided, it will be used for all logs)
    num_shops = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 5
    num_logs_per_shop = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 20
    days_back = int(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3].isdigit() else 0
    shop_id = sys.argv[4] if len(sys.argv) > 4 else (sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].isdigit() else None)
    
    target_date = datetime.now(tz=timezone.utc).date()
    if days_back == 0:
        date_range = f"TODAY ({target_date.isoformat()})"
    else:
        start_date = (datetime.now(tz=timezone.utc) - timedelta(days=days_back)).date()
        date_range = f"{start_date.isoformat()} to {target_date.isoformat()} ({days_back + 1} days)"
    
    print("=" * 60)
    print("Supabase Terminal Logs Test Data Generator")
    print("=" * 60)
    if shop_id:
        print(f"Shop ID: {shop_id}")
    else:
        print(f"Shops: {num_shops}")
    print(f"Logs per shop per day: ~{num_logs_per_shop}")
    print(f"Date range: {date_range}")
    print("=" * 60)
    print()
    
    generate_test_data(
        num_shops=num_shops,
        num_logs_per_shop=num_logs_per_shop,
        days_back=days_back,
        shop_id=shop_id
    )
