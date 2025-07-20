import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

async def test_connection():
    try:
        # Test connection
        conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
        print("✅ Database connected successfully!")
        
        # Check tables
        tables = await conn.fetch("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' 
            ORDER BY tablename
        """)
        
        print("\n📋 Tables found:")
        for table in tables:
            print(f"  - {table['tablename']}")
        
        # Check if chat history tables exist
        required_tables = ['User', 'Thread', 'Step', 'Element', 'Feedback']
        for table in required_tables:
            exists = any(t['tablename'] == table for t in tables)
            print(f"  {table}: {'✅' if exists else '❌'}")
        
        # Check Step table structure
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'Step' 
            ORDER BY ordinal_position
        """)
        
        print("\n📊 Step table columns:")
        for col in columns:
            print(f"  - {col['column_name']}: {col['data_type']} (nullable: {col['is_nullable']})")
        
        await conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_connection())