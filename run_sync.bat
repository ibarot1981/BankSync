@echo off
echo Checking for virtual environment...
IF NOT EXIST "venv\Scripts\activate.bat" (
    echo Error: Virtual environment not found. Please ensure 'venv' exists and is properly set up.
    echo You can create it using: python -m venv venv
    pause
    EXIT /B 1
)

echo Activating virtual environment...
call venv\Scripts\activate.bat

echo Running run_bank_sync.py...
python run_bank_sync.py

echo Script finished.
pause
