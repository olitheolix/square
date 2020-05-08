FROM python:3.7.7-slim

# Install Pip and Pipenv.
RUN apt update && apt install -y git
RUN pip install pipenv pip --upgrade

# Install the dependencies for Square.
COPY Pipfile Pipfile.lock ./
RUN pipenv install --system

# Copy Square into the container.
WORKDIR /square
COPY . square/

# Allow convenient execution of Square in Docker.
ENTRYPOINT ["python", "square/runme.py"]
CMD ["-h"]
