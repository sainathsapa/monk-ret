import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class CSVHandler(FileSystemEventHandler):
    def __init__(self, folder_to_watch):
        self.folder_to_watch = folder_to_watch

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            file_path = event.src_path
            print(f"📂 CSV file created: {file_path}")
            # Pass the file path as an argument to orchestrator.py
            subprocess.run(['python', 'orchestrator.py', file_path], check=True)

    # Optional: handle modifications
    # def on_modified(self, event):
    #     if not event.is_directory and event.src_path.endswith('.csv'):
    #         file_path = event.src_path
    #         print(f"✏️ CSV file modified: {file_path}")
    #         subprocess.run(['python', 'orchestrator.py', file_path], check=True)

if __name__ == "__main__":
    folder_to_watch = './csv_folder'  # Change to your folder path

    event_handler = CSVHandler(folder_to_watch)
    observer = Observer()
    observer.schedule(event_handler, folder_to_watch, recursive=False)

    observer.start()
    print(f"👀 Watching folder: {folder_to_watch} for new or modified CSV files...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
