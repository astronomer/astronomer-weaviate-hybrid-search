## Weaviate + Astronomer for hybrid search - Reference architecture

Welcome! 
This project is an end-to-end pipeline showing how to implement hybrid search in production with [Weaviate](https://weaviate.io/) and [Astronomer](https://www.astronomer.io/) 
for an eCommerce use case. You can use this project as a starting point to build your own pipelines for similar use cases.

This project was part of the [Modern Infrastructure for World Class AI Applications](https://www.astronomer.io/events/webinars/modern-infrastructure-for-world-class-ai-applications-video/) webinar, which you can watch for free!

> [!TIP]
> If you are new to Airflow, we recommend checking out our get started resources: [DAG writing for data engineers and data scientists](https://www.astronomer.io/events/webinars/dag-writing-for-data-engineers-and-data-scientists-video/) before diving into this project.

## Tools used

- [Apache Airflow®](https://airflow.apache.org/docs/apache-airflow/stable/index.html) running on [Astro](https://www.astronomer.io/product/). A [free trial](http://qrco.de/bfHv2Q) is available.
- [Weaviate](https://weaviate.io/) running on [Weaviate Cloud](https://weaviate.io/deployment/serverless). A [free trial](https://console.weaviate.cloud/) is available.
- [Amazon S3](https://aws.amazon.com/s3/) free tier. You can also adapt the pipeline to run with your preferred object storage solution.
- [OpenAI](https://platform.openai.com/docs/overview) to create embedding vectors. You need at least a Tier 1 key to run the project.
- [Flask](https://flask.palletsprojects.com/en/3.0.x/) - the demo website backend is written in flask and running in a Docker container. No accounts or setups needed.
- [React](https://react.dev/) - the demo website frontend is written in React and running in a Docker container. No accounts or setups needed.

Optional:

As a bonus, this project includes a batch inference pipeline generating insights from users' search terms and displaying them in a Streamlit app on Snowflake. To run the batch inference pipeline, you need:

- [Snowflake](https://www.snowflake.com/en/). A [free trial](https://signup.snowflake.com/) is available.

## How to setup the demo environment

Follow the steps below to set up the demo for yourself.

1. Install Astronomer's open-source local Airflow development tool, the [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview).
2. Log into your AWS account and create [a new empty S3 bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/creating-bucket.html). Make sure you have a set of [AWS credentials](https://docs.aws.amazon.com/iam/) with `AmazonS3FullAccess` for this new bucket.
3. (Optional): Sign up for a [free trial](https://signup.snowflake.com/) of Snowflake. Create a database called `hybrid_search_demo` with a schema called `dev`.

    A Snowflake account is needed to run the following DAGs, which add additional product information about sneakers and run the bonus batch inference pipeline.
    
    - [`setup_sample_data_product_info_snowflake`](dags/helper/setup_sample_data_product_info_snowflake.py), display name: `🛠️ Load sample product info to Snowflake` 
    - [`in_product_info_snowflake`](dags/ingest/in_product_info_snowflake.py), display name: `📝 Ingest product information: ❄️ -> S3`
    - [`analyze_search_terms`](dags/analytics/analyze_search_terms.py), display name: `🤖 Batch inference - Search term analysis`

    If you don't have a Snowflake account, delete these DAGs by deleting their file in the `dags` folder.

4. Fork this repository and clone the code locally.

### Run the project locally

1. Create a new file called `.env` in the root of the cloned repository and copy the contents of [.env_example](.env_example) into it. Fill out the placeholders with your own credentials for Weaviate, OpenAI, AWS and optionally, Snowflake.
2. In the root of the repository, run `astro dev start` to start up the following Docker containers. This is your local development environment.

    - Weaviate: A local Weaviate instance.
    - Postgres: Airflow's Metadata Database.
    - Webserver: The Airflow component responsible for rendering the Airflow UI. Accessible on port `localhost:8080`.
    - Scheduler: The Airflow component responsible for monitoring and triggering tasks
    - Triggerer: The Airflow component responsible for triggering deferred tasks
    - Backend: The demo website backend written in Flask.
    - Frontend: The demo website frontend written in React. Accessible at `localhost:3000`.

    Note that after any changes to `.env` you will need to run `astro dev start` for new environment variables to be picked up.

3. Access the Airflow UI at `localhost:8080` and follow the DAG running instructions in the [Running the DAGs](#running-the-dags) section of this README.

You can run and develop DAGs in this environment. Note that you might need to [provide more resources to Docker](https://docs.docker.com/engine/containers/resource_constraints/) to run this project locally.

### Run the project in the cloud

1. Sign up to [Weaviate Cloud](https://console.weaviate.cloud/) for free and [create a cluster](https://weaviate.io/developers/wcs/create-instance).
2. Sign up to [Astro](http://qrco.de/bfHv2Q) for free and follow the onboarding flow to create a deployment with default configurations.
3. Set up your Weaviate, AWS and Snowflake connections, as well as all other environment variables listed in [`.env_example](.env_example) on Astro. For instructions see [Manage Airflow connections and variables](https://www.astronomer.io/docs/astro/manage-connections-variables) and [Manage environment variables on Astro](https://www.astronomer.io/docs/astro/manage-env-vars).

4. Start the webapp project locally, by running `astro dev start`. This will spin up the following local containers:

    - Weaviate: A local Weaviate instance.
    - Postgres: Airflow's Metadata Database.
    - Webserver: The Airflow component responsible for rendering the Airflow UI. Accessible on port `localhost:8080`.
    - Scheduler: The Airflow component responsible for monitoring and triggering tasks
    - Triggerer: The Airflow component responsible for triggering deferred tasks
    - Backend: The demo website backend written in Flask.
    - Frontend: The demo website frontend written in React. Accessible at `localhost:3000`.

    If you are using cloud resources, you will only need to access the website frontend at `localhost:3000`.

5. Open the Airflow UI of your Astro deployment and follow the steps in [Running the DAGs](#running-the-dags).

## Running the DAGs

After setting up your project resources you can use the DAGs tagged with `helper` to set up the data for the demo.

1. Unpause all DAGs tagged with `helper` by clicking the toggle to the left of the DAG name (their name all starts with a 🛠️ emoji). You can use the Test Connections DAG to test your Weaviate and Snowflake connection.

![Screenshot of the Airflow UI showing the DAGs tagged with helper unpaused.](/static/helper_dags.png)

2. Run the helper DAGs starting with `Load` once. Either run them manually by clicking the play button to the right side of the screen, or by updating the `setup` Dataset. Wait until the helper DAGs finished successfully, they are moving your data into position for the demo.

![Screenshot of the Airflow UI showing the DAGs tagged with helper having completed successfully.](/static/helper_dags_finished.png)

3. Unpause all other DAGs. They will start running in their respective order based on Dataset schedules.

![Screenshot of the Airflow UI showing the DAGs tagged with use-case having completed successfully.](/static/use_case_dags_finished.png)

4. After all DAGs have completed, open the demo webapp frontend at `localhost:3000` and try out the search! Note that only the search is functional on the demo website.

![Screenshot of the Demo website main page.](/static/website_main_page.png)

![Screenshot of the Demo website advanced search page.](/static/website_advanced_search.png)

You can use the slider between `I know what I want` (full keyword search) and `Looking for ideas` (full vector search) to test different hybrid search settings.

5. Optional: If you choose to add a Snowflake connection, you can add a new [Streamlit](https://www.snowflake.com/en/data-cloud/overview/streamlit-in-snowflake/) app in your snowflake account using the demo app located in [`streamlit_app/streamlit_app.py`](streamlit_app/streamlit_app.py) to see a dashboard based on search term analytics.
