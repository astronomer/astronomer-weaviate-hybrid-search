from flask import Flask, request, jsonify
from flask_cors import CORS
import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.query import Filter
import os
import boto3

app = Flask(__name__)
CORS(app)

_S3_BUCKET = os.getenv("S3_BUCKET")
_STAGE_FOLDER_NAME = os.getenv("STAGE_FOLDER_NAME")


def instantiate_weaviate_client():

    OPENAI_KEY = os.getenv("OPENAI_KEY")
    headers = {"X-OpenAI-Api-Key": OPENAI_KEY}

    client = weaviate.connect_to_weaviate_cloud(
        cluster_url=os.getenv("WEAVIATE_URL"),
        auth_credentials=Auth.api_key(os.getenv("WEAVIATE_AUTH")),
        headers=headers,
    )

    return client


def generate_presigned_url(bucket_name, object_name, expiration=3600):
    s3_client = boto3.client("s3")

    response = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": object_name},
        ExpiresIn=expiration,
    )

    return response


@app.route("/check-collection", methods=["POST"])
def check_collection():

    client = instantiate_weaviate_client()

    data = request.get_json()
    collection_name = data.get("collection_name")
    if not collection_name:
        return jsonify({"error": "collection_name is required"}), 400

    try:
        exists = client.collections.exists(collection_name)
        return jsonify({"exists": exists})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/search", methods=["GET"])
def search():
    client = instantiate_weaviate_client()
    query = request.args.get("query")

    generative = request.args.get('generative', 'false').lower() == 'true'
    num_results = int(request.args.get("numResults", 9))
    category = request.args.get("category", None)
    alpha = float(request.args.get("alpha", 0.5))

    products = client.collections.get("Products")

    # TODO: make this work?
    if generative:
        prompt = "Given this: {description}, how would you sell it to people?"
        response = products.generate.hybrid(
            query=query,
            alpha=alpha,
            limit=1,
            single_prompt=prompt
        )

    elif category:
        response = products.query.near_text(
            query=query,
            return_properties=["title", "description", "file_path", "price"],
            filters=Filter.by_property("category").equal(category),
            limit=num_results,
        )
    else:
        response = products.query.hybrid(
            query=query,
            alpha=alpha,
            return_properties=["title", "description", "file_path", "price"],
            limit=num_results,
        )

    results = []
    for item in response.objects:
        print(item.properties["file_path"])
        presigned_url = generate_presigned_url(
            _S3_BUCKET, _STAGE_FOLDER_NAME + "/" + item.properties["file_path"]
        )
        print(presigned_url)
        result = {
            "title": item.properties["title"],
            "description": item.properties["description"],
            "file_path": presigned_url,
            "price": item.properties["price"],
        }

        if generative:
            results["generated"] = item.generated

        results.append(result)

    client.close()

    return jsonify(results)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
