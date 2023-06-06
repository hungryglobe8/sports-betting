#!/bin/sh

if command -v poetry &> /dev/null
then
  echo "Poetry detected"
  poetry install
else
  echo "Poetry not detected"
  echo "Installing now"
  #curl -sSL https://install.python-poetry.org | python3 -
  curl -sSL https://install.python-poetry.org | python3 - --git https://github.com/python-poetry/poetry.git@master
  echo "May need to restart terminal. Run setup once more or 'poetry install'"
fi
echo "'poetry run python scraper.py' to run the program"
