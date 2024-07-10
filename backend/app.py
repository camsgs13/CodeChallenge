import uvicorn
from fastapi import FastAPI
from routers.health import router
from routers.test_new_db import router_test
from routers.create_new_employee import router_create_new_employee
from routers.create_new_department import router_create_new_department
from routers.create_new_job import router_create_new_job
from routers.backup_table import router_backup_table
from routers.restore_table import router_restore_table
from routers.employees_hired_by_year_quarter import router_employees_year_quarter

app = FastAPI()
app.include_router(router)
app.include_router(router_test)
app.include_router(router_create_new_employee)
app.include_router(router_create_new_department)
app.include_router(router_create_new_job)
app.include_router(router_backup_table)
app.include_router(router_restore_table)
app.include_router(router_employees_year_quarter)

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)