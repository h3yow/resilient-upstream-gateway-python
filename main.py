"""本地验证入口：启动前构造默认故障场景的网关。"""

from __future__ import annotations

import uvicorn

from gateway.app import app

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
