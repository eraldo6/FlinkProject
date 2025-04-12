#!/bin/bash

# Simple Flink Streaming Runner
# This script starts/stops a Flink streaming job with TCP socket input

# Configuration (edit these as needed)
FLINK_HOME="$HOME/flink"
PROJECT_DIR="$HOME/SmartnicOffloading/FlinkProject/FlinkTpcDs"
JAR_PATH="$PROJECT_DIR/target/FlinkTpcDs-1.0-SNAPSHOT.jar"
MAIN_CLASS="SmartNICStreamingBenchmark"
PORT=8000
PID_FILE="/tmp/flink-stream.pid"
LOG_FILE="/tmp/flink-stream.log"
VERBOSE=false
SKIP_BUILD=false

# Function to build the project with Maven
build_project() {
  print_msg "blue" "Building project with Maven..."
  cd "$PROJECT_DIR" || exit 1
  
  if [ "$VERBOSE" = "true" ]; then
    mvn clean package
  else
    mvn clean package > maven_build.log 2>&1
  fi
  
  # Check if build was successful
  if [ $? -ne 0 ]; then
    print_msg "red" "Error: Maven build failed!"
    if [ "$VERBOSE" != "true" ]; then
      print_msg "yellow" "See maven_build.log for details."
    fi
    exit 1
  fi
  
  print_msg "green" "Build successful!"
}

# Colors for better readability
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored message
print_msg() {
  local color=$1
  local msg=$2
  case $color in
    "green") echo -e "${GREEN}$msg${NC}" ;;
    "yellow") echo -e "${YELLOW}$msg${NC}" ;;
    "red") echo -e "${RED}$msg${NC}" ;;
    "blue") echo -e "${BLUE}$msg${NC}" ;;
    *) echo "$msg" ;;
  esac
}

# Check if Flink is running
check_flink() {
  if ! $FLINK_HOME/bin/flink list &>/dev/null; then
    print_msg "red" "Error: Flink cluster is not running."
    print_msg "yellow" "Please start Flink cluster with: $FLINK_HOME/bin/start-cluster.sh"
    exit 1
  fi
  print_msg "green" "Flink cluster is running."
}

# Start the Flink job
start_job() {
  print_msg "blue" "Starting Flink streaming job on port $PORT..."
  
  # Prepare Flink command
  FLINK_CMD=(
    "$FLINK_HOME/bin/flink" "run" "-d"
    "-c" "$MAIN_CLASS"
    "-Dsocket.host=0.0.0.0"
    "-Dsocket.port=$PORT"
    "-Denable.operator.chaining=false"
    "-Dstate.checkpoints.dir=file:///tmp/flink-checkpoints"
    "-Dstate.backend.incremental=true"
    "-Dstate.checkpoints.num-retained=5"
    "-Dexecution.checkpointing.interval=60000"
    "$JAR_PATH"
    "--host" "0.0.0.0"
    "--port" "$PORT"
  )
  
  # Run the Flink job
  if [ "$VERBOSE" = "true" ]; then
    print_msg "blue" "Command: ${FLINK_CMD[*]}"
    "${FLINK_CMD[@]}" | tee -a "$LOG_FILE"
    JOB_RESULT=$?
    JOB_OUTPUT=""
  else
    JOB_OUTPUT=$("${FLINK_CMD[@]}" 2>&1)
    JOB_RESULT=$?
    echo "$JOB_OUTPUT" >> "$LOG_FILE"
  fi
  
  # Check if job submission was successful
  if [ $? -eq 0 ]; then
    # Extract the Job ID
    JOB_ID=$(echo "$JOB_OUTPUT" | grep -oP "Job has been submitted with JobID \K[a-z0-9]*")
    
    if [ -n "$JOB_ID" ]; then
      print_msg "green" "Flink job submitted successfully! JobID: $JOB_ID"
      echo "$JOB_ID" > "$PID_FILE"
      print_msg "blue" "Waiting for job to start..."
      
      # Wait for job to start (simple version)
      sleep 5
      
      JOB_STATUS=$($FLINK_HOME/bin/flink list 2>/dev/null | grep "$JOB_ID" | grep -oP "\(([A-Z]+)\)" || echo "(UNKNOWN)")
      print_msg "green" "Job status: $JOB_STATUS"
      print_msg "yellow" "TCP socket listening on port $PORT"
      print_msg "yellow" "You can now send data to any TaskManager on port $PORT"
    else
      print_msg "red" "Error: Could not extract JobID. Check Flink dashboard."
    fi
  else
    print_msg "red" "Failed to start Flink job:"
    echo "$JOB_OUTPUT"
  fi
}

# Stop the Flink job
stop_job() {
  print_msg "blue" "Stopping Flink streaming job..."
  
  if [ -f "$PID_FILE" ]; then
    JOB_ID=$(cat "$PID_FILE")
    if [ -n "$JOB_ID" ]; then
      print_msg "yellow" "Stopping job with ID: $JOB_ID"
      $FLINK_HOME/bin/flink cancel $JOB_ID &>/dev/null
      rm -f "$PID_FILE"
      print_msg "green" "Job stopped successfully."
    else
      print_msg "yellow" "No job ID found in PID file."
    fi
  else
    print_msg "yellow" "No PID file found. Checking for running jobs..."
    
    # Try to find jobs by main class name
    JOBS=$($FLINK_HOME/bin/flink list 2>/dev/null | grep "$MAIN_CLASS" | grep -oP "JobID \K[a-z0-9]*" || echo "")
    if [ -n "$JOBS" ]; then
      for JOB_ID in $JOBS; do
        print_msg "yellow" "Stopping job with ID: $JOB_ID"
        $FLINK_HOME/bin/flink cancel $JOB_ID &>/dev/null
      done
      print_msg "green" "All jobs stopped."
    else
      print_msg "yellow" "No running jobs found."
    fi
  fi
}

# Check job status
check_status() {
  print_msg "blue" "Checking job status..."
  
  if [ -f "$PID_FILE" ]; then
    JOB_ID=$(cat "$PID_FILE")
    if [ -n "$JOB_ID" ]; then
      JOB_INFO=$($FLINK_HOME/bin/flink list 2>/dev/null | grep "$JOB_ID" || echo "Job not found")
      print_msg "green" "Job status: $JOB_INFO"
    else
      print_msg "yellow" "No job ID found in PID file."
    fi
  else
    print_msg "yellow" "No PID file found. Checking for running jobs..."
    
    JOBS=$($FLINK_HOME/bin/flink list 2>/dev/null | grep "$MAIN_CLASS")
    if [ -n "$JOBS" ]; then
      print_msg "green" "Found running jobs:"
      echo "$JOBS"
    else
      print_msg "yellow" "No running jobs found."
    fi
  fi
  
  print_msg "blue" "TaskManagers in the cluster:"
  $FLINK_HOME/bin/flink list -t
}

# Show usage information
show_usage() {
  echo "Usage: $0 [options] <command>"
  echo
  echo "Commands:"
  echo "  start       Start the Flink streaming job"
  echo "  stop        Stop the running job"
  echo "  restart     Restart the job"
  echo "  status      Show job status"
  echo
  echo "Options:"
  echo "  --port PORT          Port to listen on (default: 8000)"
  echo "  --jar PATH           Path to the JAR file (default: PROJECT_DIR/target/FlinkTpcDs-1.0-SNAPSHOT.jar)"
  echo "  --flink-home PATH    Path to Flink installation"
  echo "  --project-dir PATH   Path to the project directory"
  echo "  --skip-build         Skip building the project with Maven"
  echo "  --verbose            Enable verbose output"
  echo "  --help               Show this help message"
  echo
  echo "Examples:"
  echo "  $0 start                  # Start job with default settings"
  echo "  $0 --port 9000 start      # Start job on port 9000"
  echo "  $0 --skip-build start     # Start job without rebuilding"
  echo "  $0 status                 # Check job status"
}

# Process command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --port)
      PORT="$2"
      shift 2
      ;;
    --jar)
      JAR_PATH="$2"
      shift 2
      ;;
    --flink-home)
      FLINK_HOME="$2"
      shift 2
      ;;
    --project-dir)
      PROJECT_DIR="$2"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=true
      shift
      ;;
    --verbose)
      VERBOSE=true
      shift
      ;;
    --help)
      show_usage
      exit 0
      ;;
    start|stop|restart|status)
      COMMAND="$1"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      show_usage
      exit 1
      ;;
  esac
done

# Validate required arguments
if [ -z "$COMMAND" ]; then
  print_msg "red" "Error: No command specified"
  show_usage
  exit 1
fi

if [ ! -d "$FLINK_HOME" ]; then
  print_msg "red" "Error: Flink home directory not found: $FLINK_HOME"
  exit 1
fi

# Create log directory if it doesn't exist
LOG_DIR=$(dirname "$LOG_FILE")
mkdir -p "$LOG_DIR"

# For start and restart, we need to make sure project directory exists
if [[ "$COMMAND" == "start" || "$COMMAND" == "restart" ]]; then
  if [ ! -d "$PROJECT_DIR" ] && [ "$SKIP_BUILD" != "true" ]; then
    print_msg "red" "Error: Project directory not found: $PROJECT_DIR"
    print_msg "yellow" "Please specify the correct path with --project-dir or use --skip-build"
    exit 1
  fi
  
  # Build the project if needed
  if [ "$SKIP_BUILD" != "true" ]; then
    build_project
  else
    print_msg "yellow" "Skipping build as requested"
  fi
  
  # Make sure JAR exists
  if [ ! -f "$JAR_PATH" ]; then
    print_msg "red" "Error: JAR file not found: $JAR_PATH"
    print_msg "yellow" "Please check the project build or specify the JAR path with --jar"
    exit 1
  fi
  
  print_msg "blue" "Using JAR file: $JAR_PATH"
fi

# Execute the requested command
case $COMMAND in
  start)
    check_flink
    # Stop any existing job first
    stop_job
    # Wait for job to fully stop
    print_msg "blue" "Waiting for previous job to stop (5 seconds)..."
    sleep 5
    # Start the job
    start_job
    ;;
    
  stop)
    check_flink
    stop_job
    ;;
    
  restart)
    check_flink
    stop_job
    # Wait for job to fully stop
    print_msg "blue" "Waiting for previous job to stop (10 seconds)..."
    sleep 10
    # Start the job
    start_job
    ;;
    
  status)
    check_flink
    check_status
    ;;
    
  *)
    print_msg "red" "Error: Unknown command: $COMMAND"
    show_usage
    exit 1
    ;;
esac

exit 0