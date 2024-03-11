import json

import motor as motor
import uvicorn as uvicorn
from bson import ObjectId, json_util
from fastapi import FastAPI, HTTPException, APIRouter, Request
import motor.motor_asyncio
from bson.json_util import dumps

from pymongo import MongoClient
from pydantic import BaseModel, Field
import os
import datetime

app = FastAPI()

# MongoDB setup
# client = MongoClient('mongodb://localhost:27017/')
# db = client.metadata_db

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017/')
db = client.metadata_db


# Pydantic models for request bodies
class ActivityMetadata(BaseModel):
    referenceId: str
    activity: str
    description: str
    createdBy: str
    createdDate: str
    lastModifiedDate: str
    modifiedBy: str


class BehaviourMetadata(BaseModel):
    referenceId: int
    behavior: str
    description: str
    createdBy: str
    createdDate: str
    lastModifiedDate: str
    modifiedBy: str


def serialize_doc(doc):
    """Convert MongoDB document to a JSON-serializable format."""
    doc["_id"] = str(doc["_id"])
    return doc


# using moto library
@app.post("/athena/save_experience_config/")
async def fetch_data(request: Request):
    request_body = await request.json()
    response_data = {}

    for json_key, value in request_body.items():
        collection_name = f"{json_key}_metadata"
        collection = db[collection_name]

        if value.lower() == "all":
            cursor = collection.find({})
        else:
            elements = value.split(',')
            query = {json_key: {"$in": elements}}
            cursor = collection.find(query)

        documents = []
        async for document in cursor:
            documents.append(serialize_doc(document))

        response_data[collection_name] = documents

    if response_data:
        experience_config_collection = db['experience_config']
        insert_data = {
            "data": response_data,
            "timestamp": datetime.datetime.utcnow()
        }
        await experience_config_collection.insert_one(insert_data)

    return response_data


# if motor library is not using then follow this approach to fetch any collection
@app.get("/athena/{collection_name}/")
async def get_documents_from_collection(collection_name: str):
    if collection_name not in await db.list_collection_names():
        raise HTTPException(status_code=404, detail="Collection not found")

    collection = db[collection_name]
    documents = await collection.find().to_list(None)
    return documents


# using moto library for fetching any collection
@app.get("/athena/{collection_name}")
async def experience_config_data(collection_name: str):
    collection = db[collection_name]
    documents = []
    cursor = collection.find({})

    async for document in cursor:
        documents.append(document)

    documents_json = json.loads(json_util.dumps(documents))
    return documents_json




if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
