# Crypto Data Analysis Pipeline

This project contains an ETL (Extract, Transform, Load) pipeline that fetches cryptocurrency data from CoinGecko's API, transforms it and loads it into a PostgreSQL hosted on a Azure VM running on port `5005`

## Project Features
- Stores hourly cryptocurrency data (name, price, market cap, volume, time) into a PostgreSQL database.

- Computes 6-hour and 24-hour moving averages for each coin.

- Identifies buy (local minima) and sell (local maxima) signals based on price behavior.

- Creates a daily Airflow DAG that:

   - Reads the hourly data from the database.

   - Processes moving averages and trading signals.

   - Saves results to a .csv file.

- Supports data visualization and signals in Jupyter using matplotlib with line charts and markers.

## Project Setup Instructions
### 1. Clone this repository

To clone this repository, run:
```bash
git clone https://github.com/dkkinyua/crypto-data-pipeline
cd crypto-data-pipeline
```
> NOTE: You cannot natively run Airflow on Windows, clone this repository on Windows Subsystem for Linux (WSL) or use Docker to run it on windows in an isolated environment

### 2. Install dependencies
To install the required packages, run

```bash
pip install -r requirements.txt
```

### 3. Set up environment variables
In your root directory, create a `.env` file to house your secret keys like `TEST_DB_URL`, `DB_URL` AND `API_KEY` to avoid exposing your secret keys:

```ini
TEST_DB_URL="postgresql+psycopg2://user:pass@ip_address:port/database"
DB_URL="postgresql+psycopg2://user:pass@ip_address:port/database"
API_KEY="YOUR_COINGECKO_API_KEY"
```
### 4. Set up Airflow.
To set up Airflow on your computer, set `AIRFLOW_HOME` to your root directory where your Airflow files will reside.

```bash
export AIRFLOW_HOME=your/path/to/project/airflow
```

Run the following command to initialize a database and other important Airflow files e.g. `airflow.cfg`

```bash
airflow db init
```

Create an Admin user to access your Airflow UI from the webserver:

```bash
airflow users create --username user --firstname John --lastname Doe --email j@doe.com --role Admin

# Replace these values with your credentials
```

Migrate these changes to the database:

```bash
airflow db migrate
```

Now run the following command to start your webserver and scheduler and follow the provided link to access your UI:

```bash
airflow webserver & airflow scheduler
```

Your dashboard should look something like this:

![Airflow Image](https://res.cloudinary.com/depbmpoam/image/upload/v1749047994/Screenshot_2025-06-04_173624_xolbna.png)

## Visualization and Signals

Visualization of the moving averages, minima and maxima are done in `analysis.ipynb` notebook where it shows buy/sell signal charts on different coins

In `docs/` there are csv files which contain daily analyses done on these coins which includes the 24 Hour and 6 Hour Moving Averages and Buy/Sell prices for each coin.

## Conclusion

If you would want to contribute to this project or have an issue, email me or open a pull request.

