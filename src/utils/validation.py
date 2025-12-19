"""
Configuration validation utilities
"""
import sys
import os
from typing import List, Tuple


def validate_env_vars() -> Tuple[bool, List[str]]:
    """
    Validate required environment variables

    Returns:
        Tuple of (is_valid, missing_vars)
    """
    required_vars = [
        'KAFKA_BOOTSTRAP_SERVERS',
        'MONGODB_URI',
    ]

    missing = []
    for var in required_vars:
        value = os.getenv(var)
        if not value or value == '':
            missing.append(var)

    return (len(missing) == 0, missing)


def check_env_or_exit():
    """
    Check required environment variables and exit if missing
    """
    is_valid, missing = validate_env_vars()

    if not is_valid:
        print("❌ Missing required environment variables:")
        for var in missing:
            print(f"   - {var}")
        print("\n📝 Please create a .env file with required variables.")
        print("   See .env.example for reference.\n")
        sys.exit(1)

    print("✅ Environment variables validated")
