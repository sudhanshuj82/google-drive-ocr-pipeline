"""
Production-ready OCR pipeline using Google Drive and Google Vision API

Steps:
1. Download images from Drive
2. Perform OCR using Vision API
3. Save JSONL locally
4. Upload JSONL to Drive
"""

import os
import io
import json
from datetime import datetime
from dotenv import load_dotenv

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.cloud import vision

# -------------------------
# CONFIGURATION
# -------------------------
load_dotenv()

INPUT_FOLDER_ID = os.getenv("INPUT_FOLDER_ID")
OUTPUT_FOLDER_ID = os.getenv("OUTPUT_FOLDER_ID")
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

LOCAL_IMAGE_DIR = "downloaded_images"
OUTPUT_JSONL_FILE = "google_ocr_output.jsonl"

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/cloud-vision"
]

# -------------------------
# AUTHENTICATION
# -------------------------

def get_credentials():
    """Return credentials object for Drive and Vision API"""
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES
    )
    return creds

# -------------------------
# DOWNLOAD IMAGES
# -------------------------

def download_images_from_drive(folder_id, download_dir, credentials):
    os.makedirs(download_dir, exist_ok=True)
    drive_service = build("drive", "v3", credentials=credentials)

    query = f"'{folder_id}' in parents and mimeType contains 'image/'"
    response = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = response.get("files", [])

    if not files:
        print("No images found in Drive folder.")
        return []

    image_paths = []
    print(f"Found {len(files)} images in Drive folder.")

    for file in files:
        file_path = os.path.join(download_dir, file["name"])
        request = drive_service.files().get_media(fileId=file["id"])
        with io.FileIO(file_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        image_paths.append(file_path)
        print(f"Downloaded: {file['name']}")

    return image_paths

# -------------------------
# OCR PROCESSING
# -------------------------

def run_google_ocr(image_paths, output_jsonl, credentials):
    vision_client = vision.ImageAnnotatorClient(credentials=credentials)

    with open(output_jsonl, "w", encoding="utf-8") as f:
        for image_path in image_paths:
            with open(image_path, "rb") as img:
                content = img.read()

            image = vision.Image(content=content)
            response = vision_client.text_detection(image=image)

            extracted_text = response.text_annotations[0].description if response.text_annotations else ""

            record = {
                "image_name": os.path.basename(image_path),
                "extracted_text": extracted_text,
                "processed_timestamp": datetime.utcnow().isoformat()
            }

            f.write(json.dumps(record) + "\n")
            print(f"OCR done for: {os.path.basename(image_path)}")

# -------------------------
# UPLOAD JSON TO DRIVE
# -------------------------

def upload_file_to_drive(file_path, folder_id, credentials):
    drive_service = build("drive", "v3", credentials=credentials)
    file_metadata = {"name": os.path.basename(file_path), "parents": [folder_id]}
    media = MediaFileUpload(file_path, mimetype="application/json")
    file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    print(f"Uploaded JSONL to Drive. File ID: {file.get('id')}")

# -------------------------
# MAIN PIPELINE
# -------------------------

def main():
    print("Starting OCR pipeline...")
    credentials = get_credentials()

    image_paths = download_images_from_drive(INPUT_FOLDER_ID, LOCAL_IMAGE_DIR, credentials)
    if not image_paths:
        return

    run_google_ocr(image_paths, OUTPUT_JSONL_FILE, credentials)
    upload_file_to_drive(OUTPUT_JSONL_FILE, OUTPUT_FOLDER_ID, credentials)
    print("OCR pipeline completed successfully.")

# -------------------------
# ENTRY POINT
# -------------------------

if __name__ == "__main__":
    main()
