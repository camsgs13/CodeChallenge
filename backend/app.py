import uvicorn
from fastapi import FastAPI
from routers.health import router
from routers.create_new_employee import router_create_new_employee
from routers.test_new_db import router_test

app = FastAPI()
app.include_router(router)
app.include_router(router_test)
app.include_router(router_create_new_employee)

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)