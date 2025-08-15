@echo off
setlocal enabledelayedexpansion

REM Network Rail Movement Consumer Docker Runner (Windows)
REM This script helps build and run the movement consumer container
REM Updated for modern Docker practices and better error handling

echo 🚂 Network Rail Movement Consumer
echo ==================================

REM Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo ❌ Docker is not running. Please start Docker Desktop first.
    exit /b 1
)

REM Check Docker version
for /f "tokens=3" %%i in ('docker --version') do set DOCKER_VERSION=%%i
echo ℹ️  Docker version: !DOCKER_VERSION!

REM Check if secrets.json exists
if not exist "secrets.json" (
    echo ❌ secrets.json not found. Please create it with your Network Rail credentials:
    echo    {
    echo      "username": "your_username",
    echo      "password": "your_password"
    echo    }
    exit /b 1
)

REM Create logs directory if it doesn't exist
if not exist "logs" mkdir logs
if not exist "tmp" mkdir tmp

REM Check command line argument
set "command=%1"
if "%command%"=="" set "command=run"

REM Main menu
if "%command%"=="build" goto :build
if "%command%"=="run" goto :run
if "%command%"=="logs" goto :logs
if "%command%"=="status" goto :status
if "%command%"=="stop" goto :stop
if "%command%"=="restart" goto :restart
if "%command%"=="cleanup" goto :cleanup
if "%command%"=="help" goto :help
if "%command%"=="-h" goto :help
if "%command%"=="--help" goto :help
goto :unknown

:build
echo 🔨 Building Docker image...
docker compose build --no-cache
if errorlevel 1 (
    echo ❌ Build failed!
    exit /b 1
)
echo ✅ Image built successfully!
goto :end

:run
echo 🔨 Building Docker image...
docker compose build --no-cache
if errorlevel 1 (
    echo ❌ Build failed!
    exit /b 1
)
echo 🚀 Starting movement consumer container...
docker compose up -d
if errorlevel 1 (
    echo ❌ Failed to start container!
    exit /b 1
)
echo 📊 Container started!
echo.
echo ℹ️  Useful commands:
echo    View logs:     %0 logs
echo    Stop:          %0 stop
echo    Status:        %0 status
echo    Cleanup:       %0 cleanup
goto :end

:logs
echo 📋 Showing container logs...
docker compose logs -f
goto :end

:status
echo 📊 Container status:
docker compose ps
echo.
echo ℹ️  Resource usage:
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}"
goto :end

:stop
echo 🛑 Stopping container...
docker compose down
echo ✅ Container stopped!
goto :end

:restart
echo 🔄 Restarting container...
docker compose restart
echo ✅ Container restarted!
goto :end

:cleanup
echo 🧹 Cleaning up Docker resources...
docker compose down --volumes --remove-orphans
docker system prune -f
docker image prune -f
echo ✅ Cleanup complete!
goto :end

:help
echo Usage: %0 [command]
echo.
echo Commands:
echo   build    - Build the Docker image
echo   run      - Build and run the container (default)
echo   logs     - Show container logs
echo   status   - Show container status and resource usage
echo   stop     - Stop the container
echo   restart  - Restart the container
echo   cleanup  - Stop and clean up Docker resources
echo   help     - Show this help message
goto :end

:unknown
echo ❌ Unknown command: %command%
echo Use '%0 help' for usage information
exit /b 1

:end
endlocal
