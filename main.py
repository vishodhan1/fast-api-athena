import uvicorn as uvicorn
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from pydantic import BaseModel, Field
import os

app = FastAPI()

# MongoDB setup
client = MongoClient('mongodb://localhost:27017/')
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


@app.post("/activity_metadata/")
async def insert_activity_metadata(metadata: ActivityMetadata):
    collection = db.activity_metadata
    result = collection.insert_one(metadata.dict())
    return {"_id": str(result.inserted_id)}


@app.post("/behaviour_metadata/")
async def insert_behaviour_metadata(metadata: BehaviourMetadata):
    collection = db.behaviour_metadata
    result = collection.insert_one(metadata.dict())
    return {"_id": str(result.inserted_id)}


@app.get("/activity_metadata/")
async def get_all_activity_metadata():
    collection = db["activity_metadata"]
    documents = list(collection.find({}, {"_id": 0}))
    return documents


@app.get("/behaviour_metadata/")
async def get_all_behaviour_metadata():
    collection = db["behaviour_metadata"]
    documents = list(collection.find({}, {"_id": 0}))
    return documents


@app.delete("/{collection_name}/")
async def delete_all_documents(collection_name: str):
    if collection_name not in db.list_collection_names():
        raise HTTPException(status_code=404, detail=f"Collection '{collection_name}' not found")

    collection = db[collection_name]
    result = collection.delete_many({})
    return {"message": f"Deleted {result.deleted_count} documents from {collection_name}."}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)