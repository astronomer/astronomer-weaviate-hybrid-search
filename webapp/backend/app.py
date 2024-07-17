from flask import Flask, request, jsonify
from flask_cors import CORS
import weaviate
import os

app = Flask(__name__)
CORS(app)

client = weaviate.connect_to_custom(
    http_host="weaviate",
    http_port=8081,
    http_secure=False,
    grpc_host="weaviate",
    grpc_port=50051,
    grpc_secure=False,
    headers={
        "X-OpenAI-Api-key": os.getenv("OPENAI_API_KEY")  # set in .env
    },
    auth_credentials=weaviate.auth.AuthApiKey("adminkey"),
)

@app.route('/check-collection', methods=['POST'])
def check_collection():
    data = request.get_json()
    collection_name = data.get('collection_name')
    if not collection_name:
        return jsonify({"error": "collection_name is required"}), 400

    try:
        exists = client.collections.exists(collection_name)
        return jsonify({"exists": exists})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
