# Portfolio App Backend

Backend API for Investment Portfolio Management and Analysis, built with modern FastAPI technology stack and TimescaleDB for efficient time-series data processing.

## Project Description

This application is a backend API service for an Investment Portfolio Management System that provides comprehensive portfolio tracking, transaction management, and performance analytics.

⚠️ **Warning**: The project is in development and does not currently offer a fully functional application.

### Main Functional Modules:

- **Portfolio API** - Portfolio value dynamics calculation and historical data processing
- **Transactions API** - Processing of trading operations and tracking current positions  
- **Positions API** - Management and calculation of portfolio positions with P&L
- **Prices API** - Historical and current price data processing
- **Tickers API** - Automated price data fetching and storage from Yahoo Finance
- **Upload Service** - CSV transaction data import functionality

### Technology Stack:

- **Framework**: FastAPI with async/await support and automatic OpenAPI documentation
- **Database**: PostgreSQL 17 with TimescaleDB extension for time-series optimization
- **ORM**: SQLAlchemy 2.0 with declarative models and Repository Pattern
- **Data Processing**: Pandas for transaction analysis, portfolio calculations and FX rate handling
- **Market Data**: Yahoo Finance API (yfinance) for real-time price data and ticker information
- **Configuration**: Python-decouple for environment management
- **Server**: Uvicorn ASGI Server with Hot-Reload support
- **Migrations**: Alembic for database schema versioning
- **Containerization**: Docker Compose for multi-service orchestration
- **Logging**: Structured logging with configurable log levels
- **CORS**: Configured for frontend integration with multiple origins

### Architecture Components:

#### Database Layer (TimescaleDB)
- **Hypertables**: Optimized time-series tables for `prices`, `portfolio_history` and `transactions`
- **Computed Columns**: Automatic P&L calculations at database level
- **Foreign Key Relationships**: Referential integrity between tickers, transactions and prices

#### Repository Pattern
- **Base Repository**: Common CRUD operations for all entities
- **Factory Pattern**: Central repository instantiation with Dependency Injection
- **Specialized Repositories**: Domain-specific operations for each data type

#### Service Layer
- **Portfolio Service**: Complex portfolio value calculations with multi-currency support
- **FX Rates Service**: Automatic currency conversion with Yahoo Finance FX data
- **Position Service**: Position aggregation and P&L tracking

#### API Layer
- **Modular Routing**: Separate routers for different domains
- **Pydantic Schemas**: Type-safe request/response validation
- **Dependency Injection**: Clean separation of business logic and API layer

### Key Features:

#### Time-Series Optimization
- **TimescaleDB Hypertables**: Efficient storage for millions of price data points
- **Automatic Partitioning**: Time-based data organization for optimal performance
- **Compression**: Reduced storage consumption for historical data

#### Portfolio Analytics
- **Multi-Currency Support**: Automatic FX rate conversion to base currency
- **P&L Calculations**: Realized/Unrealized gains, percentage returns
- **Position Tracking**: Real-time portfolio position calculation
- **Historical Performance**: Daily portfolio value development

#### Automated Data Management
- **Bulk Price Updates**: Efficient fetching for all portfolio tickers
- **Ticker Information**: Automatic metadata updates (currency, exchange, asset type)
- **Rate Limiting**: Controlled API calls to Yahoo Finance

#### CSV Import System
- **Data Validation**: Structured validation of transaction data
- **Bulk Operations**: Efficient processing of large CSV files
- **Error Handling**: Detailed error messages for import problems

#### Developer Features
- **Hot Reload**: Automatic reloading on code changes
- **Interactive Documentation**: Swagger UI at `/docs`
- **Structured Logging**: Configurable log levels for different components
- **Health Checks**: Database connection status monitoring

## Related Repositories

**Frontend Application**: [portfolio-app-frontend](https://github.com/vvvlladimir/portfolio-app-frontend)  
*Client-side application with user interface for portfolio management*

## Installation and Setup

### Local Development

```bash
# Clone repository
git clone https://github.com/vvvlladimir/portfolio-app.git
cd portfolio-app

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
# Create .env file with:
# DB_USER=postgres
# DB_PASSWORD=your_password
# DB_NAME=portfolio
# DB_HOST=localhost
# DB_PORT=5432

# Initialize database
python -m app.scripts.init_db

# Start development server
uvicorn app.main:app --reload
```

### Docker Setup (Recommended)

```bash
# Create .env file with database credentials
echo "DB_USER=postgres\nDB_PASSWORD=portfolio_pass\nDB_NAME=portfolio" > .env

# Start services
docker-compose up -d

# Follow logs
docker-compose logs -f backend
```

API available at [http://localhost:8000](http://localhost:8000)  
Interactive API documentation at [http://localhost:8000/docs](http://localhost:8000/docs)


## API Endpoints

### Upload Operations
- `POST /upload/transactions/csv` - Import transactions from CSV file

### Portfolio Management  
- `GET /portfolio/history` - Retrieve portfolio value development
- `GET /portfolio/transactions` - Retrieve transaction history
- `GET /portfolio/positions` - Retrieve current positions

### Price Data Management
- `GET /prices/` - Retrieve historical price data
- `POST /tickers/load-prices/{ticker}` - Load prices for specific ticker
- `POST /tickers/load-prices-all` - Update prices for all portfolio tickers

### Position Tracking
- `GET /positions/` - Detailed position data with P&L
- `GET /positions/latest` - Latest portfolio positions

### Transaction Management
- `GET /transactions/` - All transactions with filtering
- `POST /transactions/` - Add new transaction

## Database Schema

The application uses optimized time-series tables:


### TimescaleDB Hypertables
- **prices** - Historical OHLCV data, partitioned by date
- **portfolio_history** - Daily portfolio values with automatic P&L calculations


## Multi-Currency Support

The system supports automatic currency conversion:
- **Base Currency**: Configurable (default: USD)
- **FX Rate Fetching**: Automatic from Yahoo Finance
- **Supported Pairs**: All currency pairs supported by Yahoo Finance
- **Rate Caching**: Efficient storage for performance
