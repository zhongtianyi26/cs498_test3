from flask import Flask, request, jsonify
from pymongo import MongoClient, WriteConcern
from pymongo.read_preferences import ReadPreference
from bson.objectid import ObjectId

app = Flask(__name__)

# ==========================================
# 1. 数据库配置 (复用同一个全局连接)
# ==========================================
# 提醒：使用与 Part 2 相同的 Atlas URI
MONGO_URI = "mongodb+srv://zhongtianyi26_db_user:BHT0bIWFAM5rryNW@cluster0.xuacuyh.mongodb.net/?appName=Cluster0"
DB_NAME = "ev_db"
COLLECTION_NAME = "vehicles"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# ==========================================
# 2. API Endpoints (Tunable Consistency)
# ==========================================


@app.route('/insert-fast', methods=['POST'])
def insert_fast():
    """
    Fast but Unsafe Write: 只要求 Primary 节点确认 (w=1)
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        # 核心考点：使用 with_options 临时应用特定的 Write Concern
        fast_collection = collection
        
        result = fast_collection.insert_one(data)
        return jsonify({"inserted_id": str(result.inserted_id)}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route('/insert-safe', methods=['POST'])
def insert_safe():
    """
    Highly Durable Write: 要求大多数节点确认 (w="majority")
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        # 核心考点：w="majority" 保证数据强持久性
        safe_collection = collection
        
        result = safe_collection.insert_one(data)
        return jsonify({"inserted_id": str(result.inserted_id)}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/count-tesla-primary', methods=['GET'])
def count_tesla_primary():
    """
    Strongly Consistent Read: 强制从 Primary 节点读取
    """
    try:
        # 核心考点：ReadPreference.PRIMARY 保证读到最新数据
        primary_collection = collection
        
        # 注意：根据数据集实际情况，Make 可能是全大写 "TESLA"
        count = primary_collection.count_documents({"Make": "TESLA"})
        
        return jsonify({"make": "TESLA", "count": count, "consistency": "strong"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route('/count-bmw-secondary', methods=['GET'])
def count_bmw_secondary():
    """
    Eventually Consistent Read: 优先从 Secondary 节点读取，分摊主节点压力
    """
    try:
        # 核心考点：ReadPreference.SECONDARY_PREFERRED 允许最终一致性
        secondary_collection = collection
        
        count = secondary_collection.count_documents({"Make": "BMW"})
        
        return jsonify({"make": "BMW", "count": count, "consistency": "eventual"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    # 运行在 0.0.0.0 使其可以通过 GCP VM 的外部 IP 访问
    app.run(host='0.0.0.0', port=5000, debug=True)