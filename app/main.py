from fastapi import FastAPI

from routers import api_designer

app = FastAPI()

app.include_router(api_designer.router)


@app.get("/")
async def root():
    return {"message": "use /ddl to use ddl designer for snowflake!"}
