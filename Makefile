# 定义变量
PACKAGE_NAME = tapflow
VERSION = 0.2.54
MAIN_ENTRY = tapflow/cli/tap.py
DIST_DIR = dist
BUILD_DIR = build
CONFIG_DIR = $(HOME)/.tapflow

# Python 虚拟环境
VENV = .venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip

# 默认目标
.PHONY: all
all: clean setup current

# 设置虚拟环境
.PHONY: setup
setup:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install pyinstaller

# 清理构建文件
.PHONY: clean
clean:
	rm -rf $(DIST_DIR) $(BUILD_DIR)
	rm -rf *.spec

# 初始化配置文件
.PHONY: init-config
init-config:
	mkdir -p $(CONFIG_DIR)
	@if [ ! -f "$(CONFIG_DIR)/config.ini" ]; then \
		echo "[backend]" > $(CONFIG_DIR)/config.ini; \
		echo "server = localhost" >> $(CONFIG_DIR)/config.ini; \
		echo "access_code = " >> $(CONFIG_DIR)/config.ini; \
	fi

# 构建当前平台版本
.PHONY: current
current: init-config
	PYTHONPATH=$(PWD) $(PYTHON) -m PyInstaller \
		--clean \
		--name $(PACKAGE_NAME)-$(VERSION)-$(shell uname -s | tr '[:upper:]' '[:lower:]')-$(shell uname -m) \
		--add-data "requirements.txt:." \
		--add-data "README.md:." \
		--add-data "tapflow/cli/cli.py:tapflow/cli" \
		--add-data "etc:etc" \
		--add-data "tapflow:tapflow" \
		--hidden-import tapflow \
		--hidden-import tapflow.cli \
		--hidden-import tapflow.cli.cli \
		--hidden-import tapflow.cli.tap \
		--hidden-import tapflow.lib \
		--hidden-import tapflow.lib.configuration \
		--hidden-import tapflow.lib.configuration.config \
		--hidden-import tapflow.lib.backend_apis \
		--hidden-import tapflow.lib.data_pipeline \
		--hidden-import tapflow.lib.connections \
		--hidden-import IPython \
		--hidden-import yaml \
		--hidden-import requests \
		--hidden-import websockets \
		--hidden-import bson \
		--log-level ERROR \
		--onefile \
		$(MAIN_ENTRY) 