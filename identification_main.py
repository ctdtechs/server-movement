import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, desc, asc
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from urllib.parse import quote_plus
from identification_application_linux import JoblibDataManager, Only_YoloDetection
from contextlib import contextmanager
from pathlib import Path
import time
import psutil

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(BASE_DIR, '.env.identification')
if not os.path.exists(env_path):
    env_path = os.path.join(BASE_DIR, 'config', '.env.identification')
load_dotenv(dotenv_path=env_path)
app = Flask(__name__)
db = SQLAlchemy()

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = quote_plus(os.getenv("SQL_PASSWORD"))
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 18 for SQL Server")
LIMIT = os.getenv("LIMIT")
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER")

RETRY_COUNT = int(os.getenv("RETRY_COUNT", 3))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", 60))

STATUS_YET_TO_START = os.getenv("STATUS_YET_TO_START", "Yet to start").lower()
STATUS_IDENTIFICATION_COMPLETED = os.getenv(
    "STATUS_IDENTIFICATION_COMPLETED", "Completed")
STATUS_IDENTIFICATION_FAILED = os.getenv(
    "STATUS_IDENTIFICATION_FAILED", "Failed")
STATUS_AADHAAR_FOUND = os.getenv("STATUS_AADHAAR_FOUND", "Aadhar found")
STATUS_AADHAAR_NOT_FOUND = os.getenv(
    "STATUS_AADHAAR_NOT_FOUND", "Aadhar not found")
STATUS_PROCESSING_YET_TO_START = os.getenv(
    "STATUS_PROCESSING_YET_TO_START", "Yet to start")
STATUS_PROCESSING_NOT_APPLICABLE = os.getenv(
    "STATUS_PROCESSING_NOT_APPLICABLE", "Not Applicable")
STATUS_OUTPUT_PREPARATION_NOT_APPLICABLE = os.getenv(
    "STATUS_OUTPUT_PREPARATION_NOT_APPLICABLE", "Not Applicable")
WORKERS_STATUS = os.getenv("WORKERS_STATUS", "Worker 1").lower()

BATCH_COMMIT_SIZE = int(os.getenv('BATCH_COMMIT_SIZE', 50))

FILE_OPERATION = os.getenv('FILE_OPERATION', 'move').lower()
MEMORY_USAGE_THRESHOLD_PERCENT = int(
    os.getenv('MEMORY_USAGE_THRESHOLD_PERCENT'))
STORAGEPATH = os.getenv("STORAGEPATH")

attachment_dir = "attachments"

if SQL_USERNAME and SQL_PASSWORD:
    app.config["SQLALCHEMY_DATABASE_URI"] = (
        f"mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}"
        f"?driver={SQL_DRIVER}&TrustServerCertificate=yes&Encrypt=yes"
    )
else:
    app.config["SQLALCHEMY_DATABASE_URI"] = (
        f"mssql+pyodbc://@{SQL_SERVER}/{SQL_DATABASE}"
        f"?driver={SQL_DRIVER}&Trusted_Connection=yes&TrustServerCertificate=yes"
    )

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(app)

# SessionLocal = scoped_session(sessionmaker(bind=db.engine))

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_filename = f"log-{datetime.now().strftime('%Y-%m-%d')}.txt"
log_filepath = os.path.join(LOG_DIR, log_filename)

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filepath, encoding="utf-8", mode="a"),
        logging.StreamHandler()
    ]
)


class ExtractionDetail(db.Model):
    __tablename__ = "extractionDetails"
    __table_args__ = {"schema": "dbo"}

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    fileId = db.Column(db.Integer)
    orginalFileName = db.Column(db.String(255))
    extractedFileName = db.Column(db.String(255))
    extractedOn = db.Column(db.DateTime)
    extractionProcessingTime = db.Column(db.String(20))
    imageArray = db.Column(db.Text)
    identificationStatus = db.Column(db.String(50))
    identifiedOn = db.Column(db.DateTime)
    identificationProcessingTime = db.Column(db.String(20))
    binaryFilePath = db.Column(db.String(500))
    maskingStatus = db.Column(db.String(50))
    maskedOn = db.Column(db.DateTime)
    maskingProcessTime = db.Column(db.String(20))
    outputFilePrepration = db.Column(db.String(50))
    outputFileGeneratedTime = db.Column(db.DateTime)
    outputProcessingTime = db.Column(db.String(20))
    pageCount = db.Column(db.Integer, default=1)
    extractedFilePath = db.Column(db.String(500))
    pickleInputPath = db.Column(db.String(500))
    processingStatus = db.Column(db.String(500))
    workers = db.Column(db.String(500))


class FileLog(db.Model):
    __tablename__ = 'file_logs'
    __table_args__ = {"schema": "dbo"}
    log_id = db.Column(db.Integer, primary_key=True)
    log_description = db.Column(db.Text)
    log_type = db.Column(db.String)
    log_datetime = db.Column(db.DateTime, default=datetime.now)
    file_id = db.Column(db.Integer)
    log_error_code = db.Column(db.String)
    log_error_description = db.Column(db.Text)
    additional_information = db.Column(db.Text)
    worker_no = db.Column(db.String)
    failure_stage = db.Column(db.String)


class ProcessHealthCheck(db.Model):
    __tablename__ = "ProcessHealthCheck"
    __table_args__ = {"schema": "dbo"}

    Id = db.Column(db.Integer, primary_key=True)
    ProcessName = db.Column(db.String(255))
    ProcessDescription = db.Column(db.String(1000))
    Status = db.Column(db.String(50))
    ProcessLimits = db.Column(db.BigInteger)
    GetLimits = db.Column(db.BigInteger)
    IsCritical = db.Column(db.Boolean, default=False)
    ServerName = db.Column(db.String(100))
    CreatedOn = db.Column(db.DateTime)
    UpdatedOn = db.Column(db.DateTime)


def get_process_details(process_name='Identification'):
    session = SessionLocal()
    try:
        return session.query(ProcessHealthCheck).filter_by(ProcessName=process_name).first()
    except Exception as e:
        logging.error(
            f"[get_process_details] Failed to get process details: {e}")
        return None
    finally:
        session.close()


def update_process_details(status, process_description, is_critical=False, process_name='Identification'):
    session = SessionLocal()
    try:
        result = session.query(ProcessHealthCheck).filter_by(ProcessName=process_name).update({
            ProcessHealthCheck.Status: status,
            ProcessHealthCheck.ProcessDescription: process_description,
            ProcessHealthCheck.IsCritical: is_critical,
            ProcessHealthCheck.UpdatedOn: datetime.now(timezone.utc)
        })

        if result:
            session.commit()
        else:
            logging.warning(
                f"[update_process_details] No record found for ProcessName='{process_name}'")
    except Exception as e:
        session.rollback()
        logging.error(
            f"[update_process_details] Failed to update process details: {e}")
    finally:
        session.close()


class AadhaarMasking:
    def __init__(self, yolo_model_path: str, classes: list, classes2: list, gpu=False):
        self.yolo_model_path = yolo_model_path
        self.classes = classes
        self.classes2 = classes2
        self.pdf_extractor = None
        self._initialize_resources(gpu)

    def _initialize_resources(self, gpu):
        """Initialize resource-intensive objects."""
        try:
            self.pdf_extractor = Only_YoloDetection(
                self.yolo_model_path, gpu=gpu)
            self.binary_manager = JoblibDataManager()
        except Exception as e:
            self._cleanup()
            raise RuntimeError(f"Failed to initialize resources: {e}")

    def _cleanup(self):
        """Release resources held by external objects."""
        if self.pdf_extractor is not None:
            # Assuming PDFExtraction has a close method; if not, set to None
            if hasattr(self.pdf_extractor, 'close'):
                self.pdf_extractor.close()
            self.pdf_extractor = None

    def __del__(self):
        """Destructor to ensure resource cleanup."""
        self._cleanup()

    @contextmanager
    def _temporary_data(self, data):
        """Context manager to ensure temporary data is released."""
        try:
            yield data
        finally:
            data = None  # Help garbage collector release memory

    def scan(self, aadhar_scan: bool, file_path: str, threshold: int, threshold2: int):
        self.aadhar_scan = aadhar_scan
        detected_data = self.pdf_extractor.predict(
            file_path=file_path,
            classes=self.classes,
            classes2=self.classes2,
            threshold=threshold,
            threshold2=threshold2,
            aadhar_scan=aadhar_scan
        )

        with self._temporary_data(detected_data) as temp_images_data:
            return temp_images_data

    def run(self, file_path: str, output_path_binary: str, threshold: int, threshold2: int):
        try:
            images_data = self.scan(
                aadhar_scan=True,
                file_path=file_path,
                threshold=threshold,
                threshold2=threshold2
            )[0]

            binary_file_conversion = Path(file_path).with_suffix('.pkl')
            binary_file_name = binary_file_conversion.name

            if images_data:
                saved_binary_filepath = self.binary_manager.save(
                    images_data, binary_file_name, output_path_binary)
                aadhaar_occured = True
                page = [list(dict_.keys()) for dict_ in images_data]
            else:
                saved_binary_filepath, page, aadhaar_occured = "", None, False

            return page, saved_binary_filepath, aadhaar_occured

        except:
            return "", None, False
        finally:
            images_data = None


def get_yettostart_records(limit=int(LIMIT)):
    process_details = get_process_details()
    if process_details:
        return ExtractionDetail.query.filter(
            func.lower(
                ExtractionDetail.identificationStatus) == STATUS_YET_TO_START,
            func.lower(ExtractionDetail.workers) == WORKERS_STATUS
        ).limit(int(process_details.GetLimits)).all()
    else:
        return ExtractionDetail.query.filter(
            func.lower(
                ExtractionDetail.identificationStatus) == STATUS_YET_TO_START,
            func.lower(ExtractionDetail.workers) == WORKERS_STATUS
        ).limit(limit).all()


def insert_file_log(file_id=None, log_type="INFO", description=None, error_code=None,
                    error_description=None, stage=None, worker_no=None, additional_info=None):
    try:
        log = FileLog(
            file_id=file_id,
            log_type=log_type,
            log_description=description,
            log_error_code=error_code,
            log_error_description=error_description,
            failure_stage=stage,
            worker_no=worker_no,
            additional_information=additional_info,
        )
        db.session.add(log)
        db.session.commit()
    except Exception as e:
        logging.error(f"Failed to log to file_logs: {e}")


def memory_usage():
    mem = psutil.disk_usage(STORAGEPATH)
    used_percent = mem.percent
    if used_percent >= MEMORY_USAGE_THRESHOLD_PERCENT:
        logging.info(
            f"Memory usage is high ({used_percent}%). Taking action...")
        update_process_details(
            status="Ideal",
            process_description=f"Memory usage is high ({used_percent}%). Taking action...",
            is_critical=True,
            process_name='Identification'
        )
        exit()
    else:
        logging.info(
            f"Memory usage is normal ({used_percent}%).")


if __name__ == "__main__":
    with app.app_context():
        SessionLocal = scoped_session(sessionmaker(bind=db.engine))
        try:
            logging.info("Identification process started.")

            update_process_details(
                status='Running',
                process_description="Identification process started.",
                is_critical=False,
                process_name='Identification'
            )
            memory_usage()
            linux_func = AadhaarMasking(
                yolo_model_path="/data/aadhaarmask/yollox_extended_last.pt",
                classes=["Aadhar"],
                classes2=["personal_info"]
            )

            extracted_files = get_yettostart_records()

            if not extracted_files:
                logging.info("No files found.")
            else:
                processed_count = 0
                for file in extracted_files:
                    logging.info(
                        f"Filename: {file.extractedFileName} Filepath: {file.extractedFilePath}")
                    retries = RETRY_COUNT
                    for attempt in range(1, retries + 1):
                        try:
                            start_time = datetime.now()
                            insert_file_log(
                                file_id=file.id,
                                description=f"Identification started for {file.extractedFileName}",
                                log_type="INFO",
                                stage="Identification"
                            )

                            page, saved_binary_filepath, aadhaar_occured = linux_func.run(
                                file_path=file.extractedFilePath,
                                output_path_binary=OUTPUT_FOLDER,
                                threshold=0.4,
                                threshold2=91
                            )

                            end_time = datetime.now()
                            duration = (end_time - start_time).total_seconds()

                            if aadhaar_occured:
                                file.identificationStatus = STATUS_IDENTIFICATION_COMPLETED
                                file.maskingStatus = STATUS_AADHAAR_FOUND
                                file.processingStatus = STATUS_PROCESSING_YET_TO_START
                                file.binaryFilePath = str(
                                    saved_binary_filepath)
                                file.pickleInputPath = str(
                                    saved_binary_filepath)
                                file.imageArray = str(page)
                            else:
                                file.identificationStatus = STATUS_IDENTIFICATION_COMPLETED
                                file.maskingStatus = STATUS_AADHAAR_NOT_FOUND
                                file.processingStatus = STATUS_PROCESSING_NOT_APPLICABLE
                                file.outputFilePrepration = STATUS_OUTPUT_PREPARATION_NOT_APPLICABLE

                            file.identifiedOn = datetime.now()
                            file.identificationProcessingTime = f"{duration:.2f}"

                            db.session.flush()
                            processed_count += 1
                            if processed_count % BATCH_COMMIT_SIZE == 0:
                                db.session.commit()
                                logging.info(
                                    f"Batch committed {processed_count} records so far.")

                            insert_file_log(
                                file_id=file.id,
                                description=f"Identification Completed for {file.extractedFileName}",
                                log_type="INFO",
                                stage="Identification"
                            )

                            logging.info(
                                f"Completed identification for file Name: {file.extractedFileName} in {duration:.2f} seconds"
                            )

                            break

                        except Exception as e:
                            logging.error(
                                f"Attempt {attempt}: Error processing file {file.extractedFileName} - {e}")
                            insert_file_log(
                                file_id=file.id,
                                description=f"Attempt {attempt} failed: {e}",
                                log_type="ERROR",
                                stage="Identification"
                            )
                            if attempt < retries:
                                logging.info(f"Retrying after 60 seconds...")
                                time.sleep(int(RETRY_DELAY))
                            else:
                                file.identificationStatus = STATUS_IDENTIFICATION_FAILED
                                db.session.flush()
                                processed_count += 1
                                if processed_count % BATCH_COMMIT_SIZE == 0:
                                    db.session.commit()
                                    logging.info(
                                        f"Batch committed {processed_count} records so far.")
                                insert_file_log(
                                    file_id=file.id,
                                    description=f"Identification failed after {retries} attempts.",
                                    log_type="ERROR",
                                    stage="Identification"
                                )
                                logging.error(
                                    f"Identification failed for file: {file.extractedFileName}")

                # Final commit to flush any remaining records that didn't
                # reach the BATCH_COMMIT_SIZE threshold.
                if db.session.new or db.session.dirty or db.session.deleted:
                    db.session.commit()
                    logging.info(
                        f"Final batch commit done. Total processed: {processed_count}")

            update_process_details(
                status='Ideal',
                process_description="Identification process stopped.",
                is_critical=False,
                process_name='Identification'
            )

        except Exception as e:
            try:
                if db.session.new or db.session.dirty or db.session.deleted:
                    db.session.commit()
                    logging.info("Committed pending records before handling unexpected exception.")
            except Exception as commit_err:
                db.session.rollback()
                logging.error(f"Failed to commit pending records after unexpected exception: {commit_err}")
            logging.error(f"Unexpected exception occurred: {e}")
            update_process_details(
                status='Ideal',
                process_description=str(e),
                is_critical=True,
                process_name='Identification'
            )
        finally:
            logging.info("Identification process completed.")
