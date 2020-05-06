FROM olitheolix/pyinstaller-36:python377

WORKDIR /src

# Clone the repo and build the binary.
WORKDIR /src/square
COPY Pipfile Pipfile.lock ./
RUN pipenv install --system --deploy --dev
COPY . /src/square
RUN pyinstaller -n square square/__main__.py
RUN pyinstaller --noconfirm square.spec
