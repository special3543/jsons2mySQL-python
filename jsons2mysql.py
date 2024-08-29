import configparser
import sys
import os
import json
import mysql.connector
from mysql.connector import pooling
import logging
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QPushButton, QLabel,
    QFileDialog, QListWidget, QLineEdit, QMessageBox, QProgressBar
)
from PyQt5.QtCore import QThread, pyqtSignal
from concurrent.futures import ThreadPoolExecutor
import time

# Configure logging
logging.basicConfig(filename='application.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class Worker(QThread):
    progress = pyqtSignal(str)
    rate = pyqtSignal(str)
    finished = pyqtSignal(str)

    def __init__(self, file_list, pool, batch_size):
        super().__init__()
        self.file_list = file_list
        self.pool = pool
        self.batch_size = batch_size

    def run(self):
        try:
            duplicates = []
            total_files = len(self.file_list)
            loaded_files = 0
            start_time = time.time()

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = []

                for index in range(total_files):
                    file_path = self.file_list[index]
                    futures.append(executor.submit(self.process_file, file_path, duplicates, index, total_files))

                    if len(futures) >= self.batch_size or index == total_files - 1:
                        # Wait for all futures that are currently running
                        for future in futures:
                            future.result()
                        loaded_files += len(futures)
                        self.progress.emit(f'{loaded_files}/{total_files}')

                        # Calculate rate
                        elapsed_time = time.time() - start_time
                        if elapsed_time > 0:
                            rate = loaded_files / elapsed_time
                            self.rate.emit(f'Sending rate: {rate:.2f} json/s')

                        futures.clear()

                self.finished.emit(
                    "Process completed. " + f"Duplicate files: {len(duplicates)}" if duplicates else "All files sent successfully.")

        except Exception as e:
            logging.error(f"General error: {e}")
            self.finished.emit(f"Error: {e}")

    def process_file(self, file_path, duplicates, index, total_files):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                data['acikAdresModel'] = json.dumps(data['acikAdresModel'])
                data['file_path'] = file_path

            with self.pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM data_json WHERE adresNo = %s", (data['adresNo'],))
                result = cursor.fetchone()

                if result[0] > 0:
                    duplicates.append(file_path)
                    return

                sql = """
                    INSERT INTO data_json (
                        adresNo, icKapiNo, yapiKullanimAmac, maksBbTip, maksBbDurum, katNo, binaNo, binaKayitNo,
                        disKapiNo, ada, pafta, parsel, siteAdi, blokAdi, postaKodu, maksBinaNumaratajTipi,
                        acikAdresModel, tapuBagimsizBolumNo, bilesenAdi, yapiKullanimAmacFormatted, maksBbTipFormatted,
                        maksBbDurumFormatted, maksBinaNumaratajTipiFormatted, adi, kimlikNo
                    )
                    VALUES (
                        %(adresNo)s, %(icKapiNo)s, %(yapiKullanimAmac)s, %(maksBbTip)s, %(maksBbDurum)s, %(katNo)s, %(binaNo)s,
                        %(binaKayitNo)s, %(disKapiNo)s, %(ada)s, %(pafta)s, %(parsel)s, %(siteAdi)s, %(blokAdi)s,
                        %(postaKodu)s, %(maksBinaNumaratajTipi)s, %(acikAdresModel)s, %(tapuBagimsizBolumNo)s,
                        %(bilesenAdi)s, %(yapiKullanimAmacFormatted)s, %(maksBbTipFormatted)s, %(maksBbDurumFormatted)s,
                        %(maksBinaNumaratajTipiFormatted)s, %(adi)s, %(kimlikNo)s
                    )
                """
                cursor.execute(sql, data)
                conn.commit()

                os.remove(data['file_path'])  # File removal after processing

        except (IOError, json.JSONDecodeError) as file_error:
            logging.error(f"Error: An error occurred while reading the file {file_path}: {file_error}")
            self.finished.emit(f"Error: An error occurred while reading the file {file_path}: {file_error}")


class JsonToDatabaseApp(QWidget):
    def __init__(self):
        super().__init__()
        self.config_file = os.path.join(os.getenv('APPDATA'), 'db_config.ini')
        self.pool = None
        self.db_name = "adres_json_db"
        self.batch_size = 500
        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()

        self.host_input = QLineEdit(self, placeholderText='Host')
        self.user_input = QLineEdit(self, placeholderText='User')
        self.dbname_label = QLabel(f'Database: {self.db_name} (unchangeable)', self)
        layout.addWidget(self.host_input)
        layout.addWidget(self.user_input)
        layout.addWidget(self.dbname_label)

        self.db_status_label = QLabel('Not connected', self)
        layout.addWidget(self.db_status_label)

        self.connect_button = QPushButton('Connect', self)
        self.connect_button.clicked.connect(self.connect_to_db)
        layout.addWidget(self.connect_button)

        self.file_button = QPushButton('Select File Path', self)
        self.file_button.clicked.connect(self.open_file_dialog)
        layout.addWidget(self.file_button)

        self.file_list = QListWidget(self)
        self.file_list.setSelectionMode(QListWidget.NoSelection)
        layout.addWidget(self.file_list)

        self.progress_label = QLabel('Progress: 0/0', self)
        layout.addWidget(self.progress_label)

        self.rate_label = QLabel('Sending rate: 0.00 json/s', self)
        layout.addWidget(self.rate_label)

        self.send_button = QPushButton('Send', self)
        self.send_button.clicked.connect(self.send_to_database)
        layout.addWidget(self.send_button)

        self.setLayout(layout)
        self.setWindowTitle('JSON to Database')

        self.load_db_config()

    def load_db_config(self):
        if os.path.exists(self.config_file):
            config = configparser.ConfigParser()
            config.read(self.config_file)
            self.host_input.setText(config['DATABASE']['host'])
            self.user_input.setText(config['DATABASE']['user'])

    def save_db_config(self):
        config = configparser.ConfigParser()
        config['DATABASE'] = {
            'host': self.host_input.text(),
            'user': self.user_input.text(),
        }
        with open(self.config_file, 'w') as configfile:
            config.write(configfile)

    def connect_to_db(self):
        try:
            if self.pool:
                self.pool.close()

            self.pool = mysql.connector.pooling.MySQLConnectionPool(
                pool_name="mypool",
                pool_size=5,
                host=self.host_input.text(),
                user=self.user_input.text(),
                database=self.db_name
            )

            connection = self.pool.get_connection()
            cursor = connection.cursor()

            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
            cursor.execute(f"USE {self.db_name}")

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_json (
                    adresNo INT PRIMARY KEY,
                    icKapiNo VARCHAR(255),
                    yapiKullanimAmac INT,
                    maksBbTip INT,
                    maksBbDurum INT,
                    katNo VARCHAR(255),
                    binaNo INT,
                    binaKayitNo INT,
                    disKapiNo VARCHAR(255),
                    ada VARCHAR(255),
                    pafta VARCHAR(255),
                    parsel VARCHAR(255),
                    siteAdi VARCHAR(255),
                    blokAdi VARCHAR(255),
                    postaKodu VARCHAR(255),
                    maksBinaNumaratajTipi INT,
                    acikAdresModel JSON,
                    tapuBagimsizBolumNo VARCHAR(255),
                    bilesenAdi VARCHAR(255),
                    yapiKullanimAmacFormatted VARCHAR(255),
                    maksBbTipFormatted VARCHAR(255),
                    maksBbDurumFormatted VARCHAR(255),
                    maksBinaNumaratajTipiFormatted VARCHAR(255),
                    adi VARCHAR(255),
                    kimlikNo INT
                )
            """)

            connection.commit()
            cursor.close()
            connection.close()

            self.db_status_label.setText('Connected')
            self.save_db_config()
        except mysql.connector.Error as err:
            logging.error(f"Connection error: {err}")
            QMessageBox.critical(self, "Connection Error", f"Connection failed: {err}")
            self.db_status_label.setText('Not connected')
            self.pool = None

    def open_file_dialog(self):
        try:
            folder_path = QFileDialog.getExistingDirectory(self, 'Select Folder')
            if folder_path:
                self.file_list.clear()
                for file_name in os.listdir(folder_path):
                    if file_name.endswith('.json'):
                        self.file_list.addItem(os.path.join(folder_path, file_name))
        except Exception as e:
            QMessageBox.critical(self, "Error", f"An error occurred while opening files: {e}")

    def send_to_database(self):
        if not self.pool:
            QMessageBox.warning(self, "No Database Connection", "Please connect to the database.")
            return

        file_paths = [self.file_list.item(i).text() for i in range(self.file_list.count())]
        if not file_paths:
            QMessageBox.warning(self, "No File Selected", "Please select JSON files.")
            return

        self.progress_label.setText('Progress: 0/{}'.format(len(file_paths)))
        self.worker = Worker(file_paths, self.pool, self.batch_size)
        self.worker.progress.connect(self.progress_label.setText)
        self.worker.rate.connect(self.rate_label.setText)  # Connect rate signal to label
        self.worker.finished.connect(self.show_finish_message)
        self.worker.start()

    def show_finish_message(self, message):
        QMessageBox.information(self, "Process Completed", message)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = JsonToDatabaseApp()
    window.show()
    sys.exit(app.exec_())
