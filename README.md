# Coins-To-Track

## Example Directory Structure:

```
./ (ROOT)
│   .gitignore
│   Pipfile
│   Pipfile.lock
│   README.md
│   requirements.txt
│   setup_pipenv.ps1
│   setup_pipenv.sh
│
└───src
    │   .env
    │   coins_to_track.csv
    │   extract.py
    │   quote_analysis.py
    │   test.ipynb
    │
    ├───analysis
    │   ├───average_diffs
    │   │       20250102T000000_avg_relative_24h_percent_change_vs_BTC.csv
    |   |       20250103T000000_avg_relative_24h_percent_change_vs_BTC.csv
    │   │
    │   └───diffs
    │           20250102T000000_relative_24h_percent_change_vs_BTC.csv
    |           20250103T000000_relative_24h_percent_change_vs_BTC.csv
    │
    └───extracts
        ├───map
        │       20250102T000000_map.csv
        │       20250103T000000_map.csv
        │
        ├───metadata
        │       20250102T000000_metadata.csv
        │       20250103T000000_metadata.csv
        │
        └───quotes
                20250102T000000_quotes.csv
                20250103T000000_quotes.csv
```

## Prerequisites:
  - python & pip are installed locally and associated commands are available in Powershell or terminal
      - confirm pip installation with:
        ```pip --version```

  - pyenv is installed
    - This is so that we can create virtual envs based off any python version that may not already be on your system.
        - The scripts to run will be based on 3.11, if 3.11 is already on your system you can skip any pyenv installation steps!
        - If not see: https://github.com/pyenv/pyenv?tab=readme-ov-file#a-getting-pyenv for how to install on linux/mac/windows

    - If necessary confirm installation with:
    ```pyenv --version```

  - pipenv is installed
    - If not run the following in powershell or terminal:
      
      ```pip install pipenv --user```
    - confirm installation with:

      ```pipenv --version```
    
        - For Windows: 
            - If pipenv is not a registered command in Powershell run ```pip show pipenv``` to get the location of its installation
            - Bottom of output should look like:

            ```Location: C:\Users\robla\AppData\Roaming\Python\Python311\site-packages``` 

            - Add the following to your Windows PATH variable: 

            ```C:\Users\robla\AppData\Roaming\Python\Python311\Scripts```

            - reconfirm installation with ```pipenv --version```
        - For Linux:
            - If pipenv is not a registered command in terminal, try restarting a new shell. Otherwise idk... :/

## Activating Venv:
  - Using provided setup scripts:
    - If on Windows: Run ```setup_pipenv.ps1``` in powershell
    - If on Linux:  Run ```bash setup_pipenv.sh``` in terminal
    - Both of the above scripts do run the commands listed below.

  - Manual Setup:  
    - From Powershell or terminal run the following from the ROOT directory:
      - Create a pipenv project in your current directory: ```pipenv --python 3.11```
        - follow pyenv prompt to install python 3.11 if it's not already on your system 
      - Generate Pipfile & Pipfile.lock from given requirements.txt file: ```pipenv install -r requirements.txt```
    
  - From Powershell or terminal run the following from the root directory to activate the environment: 
    
    - ```pipenv shell```
    
    - Once the shell is active we can run our scripts with all the dependencies needed.

  
## Running Scripts:
  - From Powershell or terminal **run the following from within the src/ directory**:

    - Extract metadata, map, and latest price quotes for symbols in **coins_to_track.csv**: ```python extract.py```

      - respective data is written to dedicated directory as a timestamped csv w/ path format: 
      
      ```extracts/<type_of_data>/YYYYMMDDTHHMMSS_<type_of_data>.csv```

    - Run analysis to find the average difference in 24hr percent returns relative to BTC across all extracted quote data files: ```python quote_analysis.py```
      - Optionally run ```python quote_analysis.py --reference_symbol='<SYMBOL>'``` to do the same analysis against another reference symbol like (ETH for example)

      - respective data is written to dedicated directory as a timestamped csv w/ path format: 
      
      ```analysis/<type_of_data>/YYYYMMDDTHHMMSS_relative_24h_percent_change_vs_<SYMBOL>.csv```