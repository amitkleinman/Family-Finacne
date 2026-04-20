#!/bin/bash
cd "$(dirname "$0")"

echo "🚀 מפעיל דשבורד פיננסי משפחתי..."

# Check Python
if ! command -v python3 &>/dev/null; then
  echo "❌ Python3 לא מותקן. הורד מ: https://www.python.org"
  read -p "לחץ Enter לסגירה..."
  exit 1
fi

# Install Flask if needed
if ! python3 -c "import flask" 2>/dev/null; then
  echo "📦 מתקין Flask..."
  pip3 install flask --quiet
fi

# Open browser after 1.5 seconds
sleep 1.5 && open http://localhost:5001 &

python3 app.py
