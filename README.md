# Countries by GDP ETL Data Engineering Project

## Overview
Full ETL pipeline to scrape, transform, and load country GDP data from Wikipedia.

### Technologies
- Python
- pandas, numpy
- BeautifulSoup (web scraping)
- SQLite

### ETL Process
1. **Extract**: Scrape nominal GDP table from Wikipedia archive.
2. **Transform**: Clean GDP (millions to billions USD, round).
3. **Load**: Output to `Countries_by_GDP.csv` and `World_Economies.db`.
4. **Logging**: Timestamps in `etl_log.txt`.

### How to Run
```bash
cd data-engineering-project/
python etl_code.py
```

### Outputs
- `Countries_by_GDP.csv`: ~200 countries, GDP in billions USD.
- `World_Economies.db`: SQLite table `Countries_by_GDP`.
- Sample query: Top GDPs >1T (US, China, ... in etl_code.py).

### Sample Data (Top 5)
| Country       | GDP_USD_billions |
|---------------|------------------|
| United States | 26854.6         |
| China         | 19373.59        |
| Japan         | 4409.74         |
| Germany       | 4308.85         |
| India         | 3736.88         |

Last run: Check `etl_log.txt`.

