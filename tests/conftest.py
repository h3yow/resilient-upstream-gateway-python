"""公共测试夹具：日志中打印注入场景与判定依据。"""

from __future__ import annotations

import logging

import pytest


@pytest.fixture(autouse=True)
def _scenario_log(caplog):
    caplog.set_level(logging.DEBUG, logger="gateway")
    yield
