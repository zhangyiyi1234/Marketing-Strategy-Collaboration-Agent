# 核心功能:实现了一个基于FastAPI的API后端服务，主要功能包括:
# (1)创建FastAPI应用：实例化FastAPI应用并启用CORS，以支持跨域请求
# (2)环境变量设置：设置SERPER_API_KEY环境变量，用于Google搜索引擎API的访问
# (3)作业管理：通过jobs字典管理并存储作业的状态和事件，确保在多线程环境中安全访问
# (4)启动Flow:
# kickoff_flow函数接受作业ID和输入参数并调用其kickoff方法启动flow
# 在执行过程中捕获异常并更新作业状态
# 使用线程异步执行作业，允许同时处理多个请求
# (5)POST接口 /api/crew：接收客户请求，验证输入数据，生成作业ID，启动kickoff_flow函数，并返回作业ID和HTTP状态码202
# (6)GET接口 /api/crew/<job_id>：根据作业ID查询作业状态，返回作业的状态、结果和相关事件


# 导入标准库
import json
from uuid import uuid4
from threading import Thread
import os
import uvicorn
# 导入第三方库
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from utils.jobManager import append_event, get_job_by_id, update_job_by_id, ensure_schema
from tasks import app as celery_app



# 服务访问的端口
PORT = 8012


# 定义输入数据模型
class CrewRequest(BaseModel):
    customer_domain: str
    project_description: str


# 定义了一个异步函数lifespan，它接收一个FastAPI应用实例app作为参数。这个函数将管理应用的生命周期，包括启动和关闭时的操作
# 函数在应用启动时执行一些初始化操作
# 函数在应用关闭时执行一些清理操作
# @asynccontextmanager 装饰器用于创建一个异步上下文管理器，它允许在yield之前和之后执行特定的代码块，分别表示启动和关闭时的操作
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时执行：尝试连接 MySQL 并建表（失败时仍可打开文档，作业需数据库就绪）
    if not ensure_schema():
        print("警告: 未能连接 MySQL 或建表失败。请安装并启动 MySQL，创建库 crewai，"
              "或设置环境变量 MYSQL_HOST / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DATABASE。")
    print("服务初始化完成")
    # yield 关键字将控制权交还给FastAPI框架，使应用开始运行
    # 分隔了启动和关闭的逻辑。在yield 之前的代码在应用启动时运行，yield 之后的代码在应用关闭时运行
    yield
    # 关闭时执行
    print("正在关闭...")


# 实例化一个FastAPI实例
# lifespan 参数用于在应用程序生命周期的开始和结束时执行一些初始化或清理工作
app = FastAPI(lifespan=lifespan)

# 启用CORS，允许任何来源访问以 /api/ 开头的接口
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 本地 API 测试页（替代 Apifox）：启动服务后访问 http://127.0.0.1:8012/test-ui/
_static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.isdir(_static_dir):
    app.mount("/test-ui", StaticFiles(directory=_static_dir, html=True), name="test-ui")

# POST接口 /api/crew，开启一次作业运行flow
@app.post("/api/crew")
async def run_flow(request: CrewRequest):
    try:
        job_id = str(uuid4())
        inputData = {
            "customer_domain": request.customer_domain,
            "project_description": request.project_description
        }
        # 使用 Celery 调用 kickoff_flow 任务
        celery_app.send_task('tasks.kickoff_flow', args=[job_id, inputData])
        return {"job_id": job_id}
    except Exception as e:
        print(f"启动作业时出错:\n\n {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# GET接口 /api/crew/{job_id}，查询特定作业状态
@app.get("/api/crew/{job_id}")
async def get_status(job_id: str):
    job = get_job_by_id(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    # 尝试解析作业结果为JSON格式
    try:
        result_json = json.loads(str(job.result))
    except json.JSONDecodeError:
        result_json = str(job.result)

    # 返回作业ID、状态、结果和事件的JSON响应
    return {
        "job_id": job_id,
        "status": job.status,
        "result": result_json,
        "events": [{"timestamp": event.timestamp.isoformat(), "data": event.data} for event in job.events]
    }

if __name__ == '__main__':
    print(f"在端口 {PORT} 上启动服务器")
    # uvicorn是一个用于运行ASGI应用的轻量级、超快速的ASGI服务器实现
    # 用于部署基于FastAPI框架的异步PythonWeb应用程序
    uvicorn.run(app, host="0.0.0.0", port=PORT)
