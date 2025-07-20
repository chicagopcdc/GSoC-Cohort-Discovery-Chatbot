# test_supabase.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print(f"✅ 连接成功！\nPostgreSQL 版本：{record[0]}")
    conn.close()
except Exception as e:
    print(f"❌ 连接失败：{e}")