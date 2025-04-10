# SII Sentiment Analysis Project

This project is designed to scrape product reviews from Amazon and Peloton websites, store them in a PostgreSQL database, and visualize the data using a Streamlit web application.

## Project Structure

- **main.py**: This script handles the web scraping of Amazon product reviews, processes the data, and inserts it into the PostgreSQL database.
- **webapp.py**: This script creates a Streamlit web application to visualize the reviews stored in the PostgreSQL database.

## Setup Instructions

### Prerequisites

- Python 3.8 or higher
- PostgreSQL database
- Required Python packages (listed in `requirements.txt`)

### Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/SII-Sentiment-Analysis-Project.git
    cd SII-Sentiment-Analysis-Project
    ```

2. Install the required Python packages:
    ```sh
    pip install -r requirements.txt
    ```

3. Set up the PostgreSQL database and update the connection details in `main.py` and `webapp.py`.

### Running the Scripts

1. Run the web scraping script to fetch and store Amazon reviews:
    ```sh
    python main.py
    ```

2. Launch the Streamlit web application:
    ```sh
    streamlit run webapp.py
    ```

## Usage

- The web scraping script (`main.py`) will fetch product reviews from Amazon and store them in the PostgreSQL database.
- The Streamlit web application (`webapp.py`) provides a dashboard to visualize the reviews, with filters for ASIN, rating, date range, and equipment type.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgements

- BeautifulSoup for web scraping
- Streamlit for the web application
- PostgreSQL for the database
- Pandas for data manipulation

Feel free to contribute to this project by submitting issues or pull requests.
