from fastapi import FastAPI

from routers.health import router

app = FastAPI()
app.include_router(router)