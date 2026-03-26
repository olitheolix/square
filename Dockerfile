FROM python:3.14-slim

# Install UV.
RUN apt update && apt install -y git
RUN pip install uv

# Copy the repository into the container.
WORKDIR /square
COPY . /square

# Install the dependencies for Square.
RUN uv pip install . --break-system-packages --system

# Allow convenient execution of Square in Docker.
ENTRYPOINT ["python", "-m", "square"]
CMD ["-h"]
