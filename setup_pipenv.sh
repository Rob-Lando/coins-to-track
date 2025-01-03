#! /usr/bin/bash
echo 'checking if pip is installed ...'
pip --version

echo 'checking if pyenv is installed...' 
pyenv --version

echo 'Creating pipenv project ...'
pipenv --python 3.11

echo 'Creating Pipfile and Pipfile.lock from requirements.txt ...'
pipenv install -r requirements.txt

echo 'Activating virtual environment ...'
pipenv shell