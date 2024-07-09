FROM quay.io/astronomer/astro-runtime:11.6.0-base

USER root

COPY include/apache_airflow_providers_weaviate-2.0.0-py3-none-any.whl /usr/local/airflow/include/apache_airflow_providers_weaviate-2.0.0-py3-none-any.whl

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r requirements.txt

RUN /usr/local/bin/install-python-dependencies

USER astro