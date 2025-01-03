Write-Output 'checking if pip is installed ...';
pip --version;

Write-Output 'checking if pyenv is installed ...';
pyenv --version;

Write-Output 'Creating pipenv project ...';
pipenv --python 3.11;

Write-Output 'Creating Pipfile and Pipfile.lock from requirements.txt ...';
pipenv install -r requirements.txt;

Write-Output 'Activating virtual environment ...';
pipenv shell