# Google Drive OCR Pipeline

This project implements a Python-based OCR pipeline using Google Cloud Vision API.

## Workflow
1. Download images from Google Drive
2. Extract text using Vision OCR
3. Store results in JSONL format
4. Upload output back to Google Drive

## Requirements
- Google Drive API enabled
- Google Vision API enabled
- Service account with appropriate permissions

## Execution
```bash
python main.py
