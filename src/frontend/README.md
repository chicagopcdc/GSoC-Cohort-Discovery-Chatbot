# Cross-Component Interaction Analyzer Frontend

This is a Chainlit-based frontend for the Cross-Component Interaction Analyzer.

## Features

- 🔐 User authentication (username/password)
- 📤 **File upload support** - Upload your own log files for analysis
- 📊 Visual display of cross-component interactions
- 🎯 Categorization into bug patterns:
  - 🔴 Resource Leak (resource_invocation)
  - 🟡 Resource Contention (abnormal_usage)
  - 🟢 Semantic Inconsistency (shared_object)
- 💾 Chat history persistence
- 📋 **Full JSON output** - View all analysis results in JSON format

## Prerequisites

1. Make sure the backend is running:
   ```bash
   cd ../backend
   python app.py
   ```

2. Install Chainlit if not already installed:
   ```bash
   pip install chainlit
   ```

## Running the Frontend

```bash
cd furina/CS5500/src/frontend
chainlit run chainlit_app.py -w
```

The `-w` flag enables auto-reload for development.

## Usage

1. **Login**: Use one of these credentials:
   - Username: `test`, Password: `test`
   - Username: `admin`, Password: `admin`
   - Username: `user`, Password: `user`

2. **Upload Files** (Optional):
   - Drag and drop log files into the chat
   - Or click the attachment button (📎) to select files
   - Multiple files can be uploaded

3. **Commands**:
   - Type `analyze` to analyze uploaded files (or default files if none uploaded)
   - Type `clear` to remove all uploaded files
   - Type `help` to see available commands
   
4. **Results**: The analysis will show:
   - List of analyzed log files
   - **Interaction Pairs JSON** - Component relationships
   - **Categorized Interactions JSON** - Bug pattern classifications
   - Summary statistics
   - Full API response (expandable)

## File Upload

The tool supports uploading custom log files:

1. **Supported formats**: Any text-based log files
2. **Multiple files**: You can upload multiple files at once
3. **File management**: 
   - Files are stored in a temporary directory
   - Use `clear` command to remove uploaded files
   - Files are automatically cleaned up when session ends

## Default Log Files

If no files are uploaded, the tool analyzes HIVE-3335 bug logs by default:
- hadoop_namenode.log
- hadoop_datanode.log
- hive_job_log.log
- hive_log.log
- hive_cli_terminal.log

## Configuration

- Backend URL: `http://localhost:8000` (modify in `chainlit_app.py` if needed)
- Default timeout: 5 minutes for analysis

## Troubleshooting

- **Timeout errors**: The analysis may take several minutes. Increase the timeout in `chainlit_app.py` if needed.
- **Connection errors**: Ensure the backend is running on port 8000.
- **Authentication errors**: Check that you're using valid credentials.
- **File upload issues**: Ensure files are text-based and not too large. 