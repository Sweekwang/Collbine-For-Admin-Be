import os
from dotenv import load_dotenv
from supabase import create_client

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

if __name__ == "__main__":
    supabase = get_supabase_client()
    
    print("Checking terminal_logs table...")
    
    # Check total count
    try:
        result = supabase.table("terminal_logs").select("*", count="exact").limit(1).execute()
        print(f"Total records in terminal_logs: {result.count}")
        
        # Get a few sample records
        if result.count and result.count > 0:
            sample = supabase.table("terminal_logs").select("*").limit(5).execute()
            print(f"\nSample records:")
            for i, record in enumerate(sample.data[:3], 1):
                print(f"\n{i}. shop_id: {record.get('shop_id')}")
                print(f"   phone_number: {record.get('phone_number')}")
                print(f"   created_at: {record.get('created_at')}")
        else:
            print("\n⚠️  Table is empty. Run generate_test_data.py first.")
    except Exception as e:
        print(f"Error querying table: {e}")
        import traceback
        traceback.print_exc()
