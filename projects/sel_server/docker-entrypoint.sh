#!/bin/bash

uvicorn sel_server.routes:app --reload --host 0.0.0.0 --port 9000
