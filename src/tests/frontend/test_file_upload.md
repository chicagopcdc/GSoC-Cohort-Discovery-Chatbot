# Testing File Upload Feature

This guide explains how to test the file upload functionality in the Cross-Component Interaction Analyzer.

## Quick Test Steps

1. **Start the Backend**
   ```bash
   cd ../backend
   python app.py
   ```

2. **Start the Frontend**
   ```bash
   cd furina/CS5500/src/frontend
   chainlit run chainlit_app.py -w
   ```

3. **Login**
   - Use credentials: `test` / `test`

4. **Upload Test File**
   - Click the attachment button (📎) in the chat input area
   - Select `sample_upload.log` or any other log file
   - You can also drag and drop files directly into the chat

5. **Analyze**
   - Type `analyze` after files are uploaded
   - Wait for the analysis to complete

## What to Expect

### After Upload:
```
📤 Files uploaded successfully!

- sample_upload.log

Total files ready for analysis: 1

Type 'analyze' to analyze these files.
```

### After Analysis:
- **Interaction Pairs JSON**: Shows component relationships found in logs
- **Categorized Interactions JSON**: Shows bug pattern classifications
- **Summary**: Count of each pattern type
- **Full API Response**: Complete response in expandable section

## Multiple File Upload

You can upload multiple files at once:
1. Select multiple files in the file dialog
2. Or drag and drop multiple files together
3. All files will be analyzed together

## Managing Files

- Type `clear` to remove all uploaded files
- Upload new files to replace previous ones
- Files are automatically cleaned when session ends

## Sample Log Files

The repository includes:
- `sample_upload.log` - A test file with various component interactions
- You can use any text-based log files from your system

## Troubleshooting

- **File not uploading**: Check file size and format (text files only)
- **Analysis timeout**: Large files may take longer, wait a few minutes
- **No interactions found**: Ensure log files contain component names (Hive, Hadoop, Spark, etc.) 