# Currency Rates Streaming Dashboard

A real-time currency exchange rates dashboard built with Streamlit and Apache Kafka. The producer fetches the latest exchange rates from ExchangeRate-API every 30 seconds and publishes them to Kafka. The dashboard consumes those messages and displays rates in searchable, filterable tables and charts.

## Features

- Live exchange rates with USD as the base currency
- Streamlit dashboard with currency search and filtering
- Interactive bar chart and sortable data table
- Optional console consumer for inspecting Kafka messages
- Configurable API key and Kafka broker through environment variables

## How it works

```text
ExchangeRate-API -> producer.py -> Kafka topic: currency-rates
                                      |                 |
                               consumer.py         app.py -> Dashboard
```

`producer.py` sends rate updates to Kafka. `app.py` reads updates in the background and renders the dashboard. `consumer.py` is an optional command-line consumer for checking incoming messages.

## Requirements

- Python 3.9 or newer
- A running Apache Kafka broker
- An ExchangeRate-API key

## Setup

1. Create and activate a virtual environment:

   ```bash
   python -m venv .venv
   ```

   On Windows PowerShell:

   ```powershell
   .venv\Scripts\Activate.ps1
   ```

   On macOS or Linux:

   ```bash
   source .venv/bin/activate
   ```

2. Install the project dependencies:

   ```bash
   pip install streamlit kafka-python requests python-dotenv pandas plotly
   ```

3. Create your environment file by copying `.env.example` to `.env`, then add your ExchangeRate-API key. For example:

   ```env
   EXCHANGE_RATE_API_KEY=your_api_key_here
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   ```

   PowerShell:

   ```powershell
   Copy-Item .env.example .env
   ```

   macOS or Linux:

   ```bash
   cp .env.example .env
   ```

4. Start your Kafka broker and make sure its address matches `KAFKA_BOOTSTRAP_SERVERS` in `.env`. The default address is `localhost:9092`.

## Run the application

Open separate terminals (with the virtual environment activated) and start the components in this order:

1. Start the producer:

   ```bash
   python producer.py
   ```

2. Start the dashboard:

   ```bash
   streamlit run app.py
   ```

3. Optional: monitor the topic from another terminal:

   ```bash
   python consumer.py
   ```

The producer publishes a new update every 30 seconds. The dashboard may show a waiting message until the first update arrives.

## Dashboard controls

- Choose an automatic refresh interval.
- Show all currencies or select specific currency codes.
- Search the displayed rates.
- Toggle the chart and data table.

Rates are shown as units of each currency per 1 USD.

## Configuration

| Variable | Description | Default |
|---|---|---|
| `EXCHANGE_RATE_API_KEY` | API key used by the producer to access ExchangeRate-API | — |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address used by the consumers | `localhost:9092` |

Keep `.env` private. It is excluded from Git by `.gitignore`.

## Project structure

```text
.
├── app.py          # Streamlit dashboard and Kafka consumer
├── producer.py     # Fetches rates and publishes them to Kafka
├── consumer.py     # Optional console consumer
├── .env.example    # Environment variable template
├── .gitignore      # Excludes local secrets and generated files
└── README.md
```

## Troubleshooting

- **Dashboard is waiting for data:** Confirm Kafka is running, then start `producer.py` and check its output.
- **Cannot connect to Kafka:** Verify `KAFKA_BOOTSTRAP_SERVERS` and that the broker is reachable from this machine.
- **API key or API response issue:** Check `EXCHANGE_RATE_API_KEY` in `.env` and review the producer output.
- **Missing Python module:** Activate the virtual environment and install the dependencies listed above.
