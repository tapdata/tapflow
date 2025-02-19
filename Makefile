# 定义变量
PACKAGE_NAME = tapflow
VERSION ?= 0.0.0
MAIN_ENTRY = tapflow/cli/tap.py
DIST_DIR = dist
BUILD_DIR = build

# 检测操作系统
ifeq ($(OS),Windows_NT)
    PLATFORM = windows
    ifeq ($(PROCESSOR_ARCHITECTURE),AMD64)
        ARCH = x86_64
    else ifeq ($(PROCESSOR_ARCHITECTURE),x86)
        ARCH = x86
    else
        ARCH = $(PROCESSOR_ARCHITECTURE)
    endif
    PYTHON = python
    PIP = pip
    CONFIG_DIR = $(subst \,/,$(USERPROFILE))/.tapflow
    MKDIR = powershell -Command "New-Item -ItemType Directory -Force -Path"
    RMRF = powershell -Command "if (Test-Path '$(1)') { Remove-Item -Recurse -Force '$(1)' }"
    COPY = powershell -Command "Copy-Item"
else
    PLATFORM ?= $(shell uname -s | tr '[:upper:]' '[:lower:]')
    ARCH ?= $(shell uname -m)
    PYTHON = python3
    PIP = pip3
    CONFIG_DIR = $(HOME)/.tapflow
    MKDIR = mkdir -p
    RMRF = rm -rf
    COPY = cp
endif

# Python 虚拟环境
VENV = .venv

# 默认目标
.PHONY: all
all: clean setup current

# 设置虚拟环境
.PHONY: setup
setup:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install pyinstaller

# 清理构建文件
.PHONY: clean
clean:
ifeq ($(OS),Windows_NT)
	$(call RMRF,$(DIST_DIR))
	$(call RMRF,$(BUILD_DIR))
	$(call RMRF,*.spec)
else
	$(RMRF) "$(DIST_DIR)" "$(BUILD_DIR)" *.spec
endif

# 初始化配置文件
.PHONY: init-config
init-config:
	$(MKDIR) "$(CONFIG_DIR)"
ifeq ($(OS),Windows_NT)
	powershell -Command "if (-not (Test-Path '$(CONFIG_DIR)/config.ini')) { @('[backend]', 'server = localhost', 'access_code = ') | Set-Content '$(CONFIG_DIR)/config.ini' -Encoding UTF8 }"
else
	@if [ ! -f "$(CONFIG_DIR)/config.ini" ]; then \
		echo "[backend]" > "$(CONFIG_DIR)/config.ini"; \
		echo "server = localhost" >> "$(CONFIG_DIR)/config.ini"; \
		echo "access_code = " >> "$(CONFIG_DIR)/config.ini"; \
	fi
endif

# 构建当前平台版本
.PHONY: current
current: init-config
	$(PYTHON) -m PyInstaller \
		--clean \
		--name $(PACKAGE_NAME)-$(VERSION)-$(PLATFORM)-$(ARCH) \
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
		--hidden-import tapflow.lib.utils \
		--hidden-import tapflow.lib.params \
		--hidden-import tapflow.lib.data_pipeline.validation \
		--hidden-import tapflow.lib.data_services \
		--hidden-import tapflow.lib.system \
		--hidden-import tapflow.lib.cache \
		--hidden-import tapflow.lib.backend_apis.common \
		--hidden-import tapflow.lib.backend_apis.connections \
		--hidden-import tapflow.lib.backend_apis.task \
		--hidden-import tapflow.lib.backend_apis.dataVerify \
		--hidden-import tapflow.lib.backend_apis.metadataInstance \
		--hidden-import tapflow.lib.backend_apis.apiServers \
		--hidden-import IPython \
		--hidden-import yaml \
		--hidden-import requests \
		--hidden-import websockets \
		--hidden-import bson \
		--hidden-import urllib \
		--hidden-import traitlets \
		--hidden-import importlib.metadata \
		--hidden-import logging \
		--hidden-import email \
		--hidden-import xml \
		--hidden-import http \
		--hidden-import ctypes \
		--hidden-import multiprocessing \
		--hidden-import dateutil \
		--hidden-import json \
		--hidden-import asyncio \
		--hidden-import concurrent.futures \
		--hidden-import idna \
		--hidden-import urllib3 \
		--hidden-import charset_normalizer \
		--log-level ERROR \
		--onefile \
		$(MAIN_ENTRY) 