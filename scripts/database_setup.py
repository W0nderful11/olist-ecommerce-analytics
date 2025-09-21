#!/usr/bin/env python3
"""
Aster E‑Commerce (Olist) — скрипт подготовки БД
Создаёт базу `olist_analytics`, схему `olist` (пустую) и проверяет подключение.
"""

import psycopg2
import sys
import subprocess

# Database configuration (по умолчанию для локального развития)
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "olist_analytics",
    "user": "postgres",
    "password": "postgres"
}

def run_sql_command(command, description):
    """Run SQL command using psql"""
    try:
        cmd = f'PGPASSWORD={DB_CONFIG["password"]} psql -h {DB_CONFIG["host"]} -p {DB_CONFIG["port"]} -U {DB_CONFIG["user"]} -d {DB_CONFIG["dbname"]} -c "{command}"'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {description}")
            return True
        else:
            print(f"❌ {description} - Error: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ {description} - Exception: {e}")
        return False

def create_database():
    """Создать базу данных olist_analytics (если ещё нет)"""
    print("🗄️  Создаю базу olist_analytics...")
    
    # Create database
    cmd = f'PGPASSWORD={DB_CONFIG["password"]} psql -h {DB_CONFIG["host"]} -p {DB_CONFIG["port"]} -U {DB_CONFIG["user"]} -d postgres -c "CREATE DATABASE olist_analytics;"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        print("✅ База создана")
        return True
    else:
        print(f"⚠️  Возможно, база уже существует: {result.stderr}")
        return True

def create_schema():
    """Создать схему olist (если нет) в целевой БД"""
    print("📋 Создаю схему olist...")
    cmd = f'PGPASSWORD={DB_CONFIG["password"]} psql -h {DB_CONFIG["host"]} -p {DB_CONFIG["port"]} -U {DB_CONFIG["user"]} -d {DB_CONFIG["dbname"]} -c "CREATE SCHEMA IF NOT EXISTS olist;"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print("✅ Схема готова")
        return True
    else:
        print(f"❌ Ошибка создания схемы: {result.stderr}")
        return False

def verify_connection():
    """Проверить подключение к БД и наличие схемы olist"""
    print("🔍 Проверяю подключение к БД...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Check schemas
        cursor.execute("""
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name IN ('public','olist')
            ORDER BY schema_name
        """)
        tables = cursor.fetchall()
        
        print(f"✅ Подключение успешно. Доступные схемы: {', '.join(t[0] for t in tables)}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def main():
    """Main setup function"""
    print("🏪 WALMART ANALYTICS - DATABASE SETUP")
    print("=" * 50)
    
    # Step 1: Create database
    if not create_database():
        print("❌ Database creation failed")
        return False
    
    # Step 2: Create schema
    if not create_schema():
        print("❌ Schema creation failed")
        return False
    
    # Step 3: Verify connection
    if not verify_connection():
        print("❌ Connection verification failed")
        return False
    
    print("\n🎉 Database setup completed successfully!")
    print("Next step: Run import_walmart_data.py to import data")
    return True

if __name__ == "__main__":
    main()
