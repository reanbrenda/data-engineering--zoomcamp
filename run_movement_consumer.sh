#!/bin/bash

# Network Rail Movement Consumer Docker Runner
# This script helps build and run the movement consumer container
# Updated for modern Docker practices and better error handling

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

echo "🚂 Network Rail Movement Consumer"
echo "=================================="

# Check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker Desktop first."
        exit 1
    fi
    
    # Check Docker version
    DOCKER_VERSION=$(docker --version | cut -d' ' -f3 | cut -d',' -f1)
    print_status "Docker version: $DOCKER_VERSION"
    
    # Check Docker Compose version
    if command -v docker-compose &> /dev/null; then
        COMPOSE_VERSION=$(docker-compose --version | cut -d' ' -f3 | cut -d',' -f1)
        print_status "Docker Compose version: $COMPOSE_VERSION"
    else
        print_warning "docker-compose not found, using 'docker compose' (Docker Compose V2)"
    fi
}

# Check if secrets.json exists and is valid
check_credentials() {
    if [ ! -f "secrets.json" ]; then
        print_error "secrets.json not found. Please create it with your Network Rail credentials:"
        echo "   {"
        echo "     \"username\": \"your_username\","
        echo "     \"password\": \"your_password\""
        echo "   }"
        exit 1
    fi
    
    # Validate JSON format
    if ! python3 -m json.tool secrets.json > /dev/null 2>&1; then
        print_error "secrets.json is not valid JSON. Please check the format."
        exit 1
    fi
    
    print_success "Credentials file validated"
}

# Create necessary directories
setup_directories() {
    mkdir -p logs
    mkdir -p tmp
    print_success "Directories created"
}

# Function to build the image
build_image() {
    print_status "Building Docker image..."
    
    # Use appropriate docker compose command
    if command -v docker-compose &> /dev/null; then
        docker-compose build --no-cache
    else
        docker compose build --no-cache
    fi
    
    if [ $? -eq 0 ]; then
        print_success "Image built successfully!"
    else
        print_error "Build failed!"
        exit 1
    fi
}

# Function to run the container
run_container() {
    print_status "Starting movement consumer container..."
    
    # Use appropriate docker compose command
    if command -v docker-compose &> /dev/null; then
        docker-compose up -d
    else
        docker compose up -d
    fi
    
    if [ $? -eq 0 ]; then
        print_success "Container started!"
        echo ""
        print_status "Useful commands:"
        echo "   View logs:     $0 logs"
        echo "   Stop:          $0 stop"
        echo "   Status:        $0 status"
        echo "   Cleanup:       $0 cleanup"
    else
        print_error "Failed to start container!"
        exit 1
    fi
}

# Function to show logs
show_logs() {
    print_status "Showing container logs..."
    
    # Use appropriate docker compose command
    if command -v docker-compose &> /dev/null; then
        docker-compose logs -f
    else
        docker compose logs -f
    fi
}

# Function to show status
show_status() {
    print_status "Container status:"
    
    # Use appropriate docker compose command
    if command -v docker-compose &> /dev/null; then
        docker-compose ps
    else
        docker compose ps
    fi
    
    echo ""
    print_status "Resource usage:"
    docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}"
}

# Function to stop the container
stop_container() {
    print_status "Stopping container..."
    
    # Use appropriate docker compose command
    if command -v docker-compose &> /dev/null; then
        docker-compose down
    else
        docker compose down
    fi
    
    print_success "Container stopped!"
}

# Function to clean up
cleanup() {
    print_status "Cleaning up Docker resources..."
    
    # Use appropriate docker compose command
    if command -v docker-compose &> /dev/null; then
        docker-compose down --volumes --remove-orphans
    else
        docker compose down --volumes --remove-orphans
    fi
    
    docker system prune -f
    docker image prune -f
    
    print_success "Cleanup complete!"
}

# Function to restart the container
restart_container() {
    print_status "Restarting container..."
    
    # Use appropriate docker compose command
    if command -v docker-compose &> /dev/null; then
        docker-compose restart
    else
        docker compose restart
    fi
    
    print_success "Container restarted!"
}

# Main execution
main() {
    check_docker
    check_credentials
    setup_directories
    
    case "${1:-run}" in
        "build")
            build_image
            ;;
        "run")
            build_image
            run_container
            ;;
        "logs")
            show_logs
            ;;
        "status")
            show_status
            ;;
        "stop")
            stop_container
            ;;
        "restart")
            restart_container
            ;;
        "cleanup")
            cleanup
            ;;
        "help"|"-h"|"--help")
            echo "Usage: $0 [command]"
            echo ""
            echo "Commands:"
            echo "  build    - Build the Docker image"
            echo "  run      - Build and run the container (default)"
            echo "  logs     - Show container logs"
            echo "  status   - Show container status and resource usage"
            echo "  stop     - Stop the container"
            echo "  restart  - Restart the container"
            echo "  cleanup  - Stop and clean up Docker resources"
            echo "  help     - Show this help message"
            ;;
        *)
            print_error "Unknown command: $1"
            echo "Use '$0 help' for usage information"
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"
