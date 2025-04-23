# Facebook API to JSON

A comprehensive Python toolkit for interacting with Facebook's Marketing API. Features include:
- Fetching and storing offsite conversion data
- Retrieving ad creative details
- ETL pipeline for data processing
- Streamlit dashboard for data visualization
- SQLite database integration

## Features

- **Data Fetching**
  - Offsite conversion data with attribution windows
  - Ad creative information (images, CTAs, etc.)
  - Campaign and ad set metrics
  
- **Data Processing**
  - ETL pipeline for data transformation
  - SQLite database for structured storage
  - Configurable data filters and aggregations

- **Visualization**
  - Streamlit dashboard for data exploration
  - Interactive charts and metrics
  - Custom date range selection

## Project Structure

```
├── src/                    # Source code
│   ├── config/            # Configuration files
│   ├── etl/               # ETL processing scripts
│   ├── examples/          # Example usage scripts
│   ├── facebook_api.py    # Facebook API interface
│   ├── fb_client.py       # Facebook client implementation
│   ├── main_etl.py        # Main ETL pipeline
│   ├── streamlit_app.py   # Streamlit dashboard
│   ├── database.py        # Database operations
│   └── config.py          # Configuration management
├── .env.example           # Example environment variables
├── .gitignore             # Git ignore rules
├── LICENSE                # MIT License
├── README.md             # This file
├── pyproject.toml        # Project metadata
└── requirements.txt      # Python dependencies
```

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd facebook_api_to_json
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
   - Copy `.env.example` to `.env`
   - Fill in your Facebook API credentials

## Usage

### Data Fetching
```python
from src.facebook_api import FacebookAPI

api = FacebookAPI()
conversions = api.fetch_offsite_conversions()
creatives = api.fetch_ad_creatives()
```

### Running ETL Pipeline
```bash
python src/main_etl.py
```

### Starting Dashboard
```bash
streamlit run src/streamlit_app.py
```

## Configuration

Key configuration options in `.env`:
```
FACEBOOK_APP_ID=your_app_id
FACEBOOK_APP_SECRET=your_app_secret
FACEBOOK_ACCESS_TOKEN=your_access_token
FACEBOOK_AD_ACCOUNT_ID=act_your_ad_account_id
FACEBOOK_API_VERSION=v19.0
LOG_LEVEL=INFO
```

## Error Handling

The toolkit includes comprehensive error handling for:
- API authentication failures
- Network issues
- Data validation
- Rate limiting
- Database operations

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
