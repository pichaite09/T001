# Android Automation Studio

Desktop application for Android automation built with:

- `PySide6`
- `uiautomator2`
- `sqlite`

## Documentation

- User guide: `docs/USER_GUIDE.md`
- Developer guide: `docs/DEVELOPER_GUIDE.md`

## Install

```powershell
pip install -r requirements.txt
```

## Run The Main App

```powershell
python main.py
```

## Run The Upload API

```powershell
python -m automation_studio.api_server --db-path automation_studio.db --host 127.0.0.1 --port 8000
```

## Run Screen Wall Process Directly

```powershell
python -m automation_studio.viewer_process --db-path automation_studio.db --refresh-ms 1000
```

## Test

```powershell
python -m unittest discover -s tests -v
```
