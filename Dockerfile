FROM python:3.9.5

WORKDIR /app

# Set environment variables to prevent Python from writing pyc files
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH=${PYTHONPATH}:${PWD} 

# Create an airflow user with UID and GID 50000
RUN groupadd -g 50000 airflow && useradd -u 50000 -g 50000 -ms /bin/bash airflow

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Add Poetry's bin directory to the PATH
ENV PATH="${PATH}:/root/.local/bin"

# Copy and install Poetry dependencies
COPY ./pyproject.toml ./poetry.lock ./
RUN poetry install --no-root

# Copy the script you want to run with Airflow
COPY . .

CMD ["poetry", "install"]
#CMD ["bash", "-c", "sleep infinity"]
