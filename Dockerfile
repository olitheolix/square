FROM python:3.10-slim

# Install Pip and Pipenv.
RUN apt update && apt install -y git
RUN pip install pipenv pip --upgrade

# Copy the repository into the container.
WORKDIR /square
COPY . /square

# Install the dependencies for Square.
RUN pipenv install --system

# Allow convenient execution of Square in Docker.
ENTRYPOINT ["python", "runme.py"]
CMD ["-h"]
