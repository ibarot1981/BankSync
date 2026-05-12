@echo off
setlocal
cd /d "D:\Irshad\Dev\Python\BankUpdate"

set "PYTHON_EXE=python"
IF EXIST "venv\Scripts\python.exe" (
    set "PYTHON_EXE=venv\Scripts\python.exe"
)

set "PYTHONPATH=%CD%\src;%PYTHONPATH%"
set "BANKUPDATE_ARGS=%*"

IF "%BANKUPDATE_ARGS%"=="" (
    set "BANKUPDATE_ARGS=daily"
)

echo Running BankUpdate with arguments: %BANKUPDATE_ARGS%
"%PYTHON_EXE%" -m bankupdate %BANKUPDATE_ARGS%
exit /b %ERRORLEVEL%
