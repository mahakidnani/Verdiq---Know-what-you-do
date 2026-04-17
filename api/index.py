import os
import sys

# Ensure Vercel mounts the correct root for explicit backend module resolution
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.main import app
