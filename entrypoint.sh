#!/bin/bash
set -e

echo "⏳ Waiting for PostgreSQL to be ready..."
while ! pg_isready -h postgres -p 5432 -U airflow > /dev/null 2>&1; do
    sleep 1
done
echo "✅ PostgreSQL is ready."

echo "🔄 Running DB migration..."
airflow db migrate

# Only try to create admin user if no users exist
if ! airflow users list | grep -q admin; then
  echo "👤 Creating default admin user..."
  airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
else
  echo "ℹ️ Admin user already exists. Skipping creation."
fi

echo "🚀 Starting Airflow: $@"
exec "$@"
