# Portfolio App Backend

Backend API for investment portfolio management and analysis, built with modern FastAPI technology stack and TimescaleDB for efficient time-series data handling.

## Project Description

This application is a backend API service for an investment portfolio management system that provides comprehensive portfolio tracking, transaction management, and performance analytics.

⚠️ **Warning**: The project is under development and does not provide a fully functional application at this time.

### Main Functional Modules:

- **Portfolio API** - portfolio value dynamics calculation and historical data management
- **Transactions API** - trading operations processing and current positions tracking
- **Ticker Service** - automated price data fetching and storage from Yahoo Finance
- **Upload Service** - CSV transaction data import functionality

### Technology Stack:

- **Framework**: FastAPI with async support
- **Database**: PostgreSQL with TimescaleDB extension for time-series data
- **ORM**: SQLAlchemy 2.0 with declarative models
- **Data Processing**: Pandas for transaction analysis and calculations
- **Market Data**: Yahoo Finance API (yfinance) for real-time price fetching
- **Configuration**: Python-decouple for environment management
- **Server**: Uvicorn ASGI server

### Key Features:

- **Time-series Optimization**: TimescaleDB hypertables for efficient price and portfolio history storage
- **Automated Price Updates**: Background price fetching for all portfolio tickers
- **Portfolio Analytics**: P&L calculations, return metrics, and position tracking
- **CSV Import**: Bulk transaction import with data validation
- **CORS Support**: Configured for frontend integration
- **RESTful API**: OpenAPI/Swagger documentation

## Related Repositories

**Frontend Application**: [portfolio-app-frontend](https://github.com/vvvlladimir/portfolio-app-frontend)  
*Client-side application providing user interface for portfolio management*

## Installation and Setup

```bash
# Clone repository
git clone https://github.com/vvvlladimir/portfolio-app.git
cd portfolio-app

# Install dependencies
pip install -r requirements.txt

# Setup environment variables
# Create .env file with:
# DB_USER=postgres
# DB_PASSWORD=your_password
# DB_NAME=portfolio

# Initialize database
python -m app.database

# Run development server
uvicorn main:app --reload
```

API will be available at [http://localhost:8000](http://localhost:8000)  
Interactive API documentation at [http://localhost:8000/docs](http://localhost:8000/docs)

## Requirements

- Python 3.10+
- PostgreSQL 14+ with TimescaleDB extension
- Internet connection for price data fetching

## API Endpoints

- `POST /upload/csv` - Import transactions from CSV file
- `GET /portfolio/history` - Get portfolio value dynamics
- `GET /portfolio/transactions` - Get transaction history
- `GET /portfolio/positions` - Get current positions
- `POST /ticker/load-prices/{ticker}` - Load prices for specific tickers
- `POST /ticker/load-prices-all` - Update prices for all portfolio tickers

## Database Schema

The application uses optimized time-series tables:
- **transactions** - Trading operations record
- **prices** - Historical price data (TimescaleDB hypertable)
- **portfolio_history** - Daily portfolio values (TimescaleDB hypertable)
- **positions** - Current holdings and P&L calculations
- **tickers** - Instrument metadata and exchange information
