from fastapi import FastAPI
from routers.health import router
from routers.create_new_employee import router_create_new_employee, router_test

app = FastAPI()
app.include_router(router)
app.include_router(router_test)
app.include_router(router_create_new_employee)