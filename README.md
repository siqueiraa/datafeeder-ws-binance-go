# Datafeeder WebSocket for Binance in Go

Datafeeder is a WebSocket-based tool written in Go for fetching candle data from Binance. It utilizes the [go-binance](https://github.com/adshao/go-binance/v2) library for WebSocket communication and PostgreSQL for efficient bulk data insertion.

## Features

- WebSocket integration with Binance for fetching candle data.
- Efficient bulk insertion of data into PostgreSQL database.
- Future enhancements: retrieving Open Interest (OI) and Long/Short Ratio (LSR) data.
- Sanitization of data to ensure integrity.

## Installation

1. **Clone the repository:**

    ```bash
    git clone https://github.com/yourusername/datafeeder-ws-binance-go.git
    cd datafeeder-ws-binance-go
    ```

2. **Install dependencies:**

    ```bash
    go mod tidy
    ```

3. **Set up the PostgreSQL database:**

   - Create a database and configure access in `config.yaml`.

4. **Run the program:**

    ```bash
    go run main.go
    ```

## Configuration

The configuration is managed through `config.yaml`. Modify this file to set up database connections, API keys, and other settings.

Example `config.yaml`:

```yaml
user: user
password: password
host: locahost
port: 5432
dbname: dbname
sslmode: disable
logLevel: error # or "debug", "info", etc.


Contributing
Feel free to contribute by opening issues or pull requests. Your feedback and suggestions are highly appreciated.

License
This project is licensed under the MIT License.


