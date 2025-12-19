# Changelog

## [1.0.0] - 2024-12-09

### 🔐 Security
- **Removed hardcoded credentials** from source code
- Environment variables now **required** (no default values for sensitive data)
- Added `.env` to `.gitignore`
- Created `.env.example` as template

### ✨ Features
- **Remote server support** via `docker-compose.remote.yml`
- **Environment validation** on startup
- **Connection testing tool** (`test_connection.py`)
- Support for remote Kafka and MongoDB servers

### 📝 Configuration Changes
- `KAFKA_BOOTSTRAP_SERVERS` - **Required** (no default)
- `MONGODB_URI` - **Required** (no default)
- Other settings have safe defaults

### 🗑️ Removed
- Hardcoded server IPs from code
- Legacy files and documentation
- Redundant scripts
- `.env` file (moved to `.env.example`)

### 📚 Documentation
- Added `SETUP.md` - Complete setup guide
- Updated `README.md` - Remote server instructions
- Added validation utilities

### 🔧 Technical Details
- Fixed logger deadlock issue
- Fixed Kafka `acks` parameter type
- Added startup validation checks
- Clean Architecture maintained

### 📦 Files Added
- `.env.example` - Configuration template
- `docker-compose.remote.yml` - Remote deployment
- `SETUP.md` - Setup documentation
- `test_connection.py` - Connection testing
- `src/utils/validation.py` - Environment validation

### 📊 Stats
- **20 Python files** (clean, focused)
- **296KB** total size (lightweight)
- **Zero hardcoded credentials** (secure)
- **100% environment-driven** configuration
