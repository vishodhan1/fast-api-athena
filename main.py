import json
import uuid

import motor as motor
import uvicorn as uvicorn
from bson import ObjectId, json_util
from fastapi import FastAPI, HTTPException, APIRouter, Request, UploadFile, File
import motor.motor_asyncio
from bson.json_util import dumps
import pandas as pd
from io import StringIO
from datetime import datetime


from pymongo import MongoClient
from pydantic import BaseModel, Field
import os

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
    import datetime
    if response_data:
        experience_config_collection = db['experience_config']
        insert_data = {
            "data": response_data,
            "timestamp": datetime.datetime.utcnow()
        }
        await experience_config_collection.insert_one(insert_data)

    return response_data


# if motor library is not using then follow this approach to fetch any collection
# @app.get("/athena/{collection_name}/")
# async def get_documents_from_collection(collection_name: str):
#     if collection_name not in await db.list_collection_names():
#         raise HTTPException(status_code=404, detail="Collection not found")
#
#     collection = db[collection_name]
#     documents = await collection.find().to_list(None)
#     return documents


# using moto library for fetching any collection
@app.get("/athena/{collection_name}")
async def experience_config_data(collection_name: str):
    print("hi from athena")
    collection = db[collection_name]
    print("collection", collection)
    documents = []
    cursor = collection.find({})

    async for document in cursor:
        documents.append(document)

    documents_json = json.loads(json_util.dumps(documents))
    return documents_json


@app.post("/athena/upload-csv/")
async def upload_csv(file: UploadFile = File(...)):
    if file.content_type != 'text/csv':
        raise HTTPException(status_code=400, detail="File must be a CSV.")
    collection = db["content_metadata"]
    content = await file.read()
    df = pd.read_csv(StringIO(str(content, 'utf-8')))

    records = []
    for _, row in df.iterrows():
        print("row", row)
        print(df.iterrows() )
        document_name = row.get('DocumentName', '')

        # Ensuring proper handling of potential NaN values
        zip_code = row.get('postalCode', '')
        zip_code = None if pd.isna(zip_code) else zip_code

        city_value = row.get('city', '')
        city_value = None if pd.isna(city_value) else city_value
        uniqueTitle = row.get("uniqueTitle", "")
        document = {
            "_id": str(uuid.uuid4()),
            "uniqueTitle": uniqueTitle,
            "contentReferenceId": f"{row.get('AthenaType', '')}_{uniqueTitle}",
            "contentType": row.get('Type', 'UNKNOWN'),
            "zip": zip_code,
            "city": city_value,
            "state": row.get('state', ''),
            "score": row.get('score', ''),
            "country": row.get('country', ''),
            "region": row.get('region', ''),
            "continent": row.get('continent', ''),
            "parentId": row.get('parentId', ''),
            "satelliteCities": [city.strip() for city in row.get('satelliteCities', '').split(',')] if isinstance(row.get('satelliteCities'), str) else [],
            "latLong": {"longitude": "", "latitude": ""},
            "createdBy": "Joe Black",
            "createdDate": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            "lastModifiedDate": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            "modifiedBy": "joeJ@mondee.com"
        }
        records.append(document)

    if records:
        collection.insert_many(records)
        return {"message": f"{len(records)} documents inserted into content_metadata."}
    else:
        return {"message": "No data found in the CSV file."}


@app.delete("/delete-all/{collection_name}/")
async def delete_all_documents(collection_name: str):
    # Check if the collection exists to prevent errors
    if collection_name not in await db.list_collection_names():
        raise HTTPException(status_code=404, detail="Collection not found")

    try:
        # Access the collection
        collection = db[collection_name]
        result = await collection.delete_many({})

        return {"message": f"Successfully deleted {result.deleted_count} documents from '{collection_name}'."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
