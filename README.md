## Coins-To-Track

### Prerequisites:
  - python & pip is installed locally

  - pyenv is installed
    - If not see: https://github.com/pyenv/pyenv?tab=readme-ov-file#a-getting-pyenv for how to install on linux/mac/windows

    - confirm installation with:
    ```pyenv --version```

  - pipenv is installed
    - If not run the following in powershell or terminal:
      
      ```pip install pipenv --user```
    - confirm installation with:

      ```pip show pipenv``` or ```pipenv --version```

### Activating Venv:
  - From Powershell or terminal run the following from the root directory:
    - Create a pipenv project in your current directory: ```pipenv --python 3.11```
      - follow pyenv prompt to install python 3.11 if it's not already on your system 
    - Generate Pipfile & Pipfile.lock from given requirements.txt file: ```pipenv install -r requirements.txt```
    - Activate virtual environment: ```pipenv shell```
  
### Running Scripts:
  - From Powershell or terminal run the following from within the src/ directory:

    - Extract metadata, map, and latest price quotes for symbols in **coins_to_track.csv**: ```python extract.py```

      - respective data is written to dedicated directory as a timestamped csv w/ path format: ```extracts/<type_of_data>/YYYYMMDDTHHMMSS_<type_of_data>.csv```

    - Run analysis to find the average difference in 24hr percent returns relative to BTC across all extracted quote data files: ```python quote_analysis.py```
      - Optionally run ```python quote_analysis.py --reference_symbol='<SYMBOL>'``` to do the same analysis against another reference symbol like (ETH for example)

      - respective data is written to dedicated directory as a timestamped csv w/ path format: ```analysis/<type_of_data>/YYYYMMDDTHHMMSS_relative_24h_percent_change_vs_<SYMBOL>.csv```

  