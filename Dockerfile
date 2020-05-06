FROM olitheolix/pyinstaller-36:python377

WORKDIR /src/square

# Install the dependencies into the system, not a virtual environment.
COPY Pipfile Pipfile.lock ./
RUN pipenv install --system --dev

# Copy the repository into the container.
COPY . /src/square

# Build the binary.
RUN pyinstaller square.spec --clean --distpath dist/
