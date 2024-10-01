import os
import pyodbc as db
import pyrfc as sap
import time
import datetime as dt
import configparser as cp
from tkinter import messagebox

APP_PATH = os.getcwd()
DEBUG_PATH = "\\SyncDetalleFactura"
CONFIG_PATH = f"{APP_PATH}{DEBUG_PATH}\\config.ini"
LOG_PATH = f"{APP_PATH}{DEBUG_PATH}\\applog\\"
REMOTE_FUNCTION = 'ZSINC_TRANS_DATA_STELLAR_SAP'
TIME_FORMAT = '%H:%M:%S' #'%I:%M:%S %p' Hora no militar
SIMPLE_DATE_FORMAT = '%Y%m%d'
REVERSE_DATE_FORMAT = '%Y-%m-%d'
DATE_FORMAT = '%d-%m-%Y'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_DATETIME_FORMAT = '%d/%m/%Y %I:%M:%S %p'
SAP_DATE_FORMAT = '%d.%m.%Y'

class DetalleFactura:

    def __init__(self) -> None:
        self.create_missing_directories()
        self.date = None
        self.config = self.read_config()
        self.dbcon = None
        self.sapcon = None
        self.t_ventas_st = []
        self.exec_ui = False
    
    ###########################################################
    # Repositorio para impresión de mensajes de la aplicación #
    def text_message(self, message_number): # ejemplo: 'e001', 's002', 'i007'
        type = str(message_number[0]).lower()   # Primera letra indica el tipo de mensaje
        number = message_number[1:4]            # Demas letras indican el numero del mensaje

        messages = {
            's': { # Success
                '001': 'Transacciones del día {} sincronizadas con éxito'
                }.get(number, 'No existe el mensaje "{}"'.format(message_number)),

            'e': { # Error
                '001': 'No hay información a sincronizar',
                '002': 'No se ha establecido conexión con la BD: Cursor vacio\n',
                '003': 'SUBRC = {} | {}',
                }.get(number, 'No existe el mensaje "{}"'.format(message_number)),
            
            'w': {
                '001': 'Reintentando conexión con SAP en {} segundos',
                '002': '\nReintentando conexión con SAP...',
                '003': 'Reintentando conexión con la BD en {} segundos',    
                '004': '\nReintentando conexión con la BD...',
                }.get(number, 'No existe el mensaje "{}"'.format(message_number)), 

            'i': { # Information
                '001': 'Iniciado:   {}',
                '002': 'Finalizado: {}',
                }.get(number, 'No existe el mensaje "{}"'.format(message_number)),
        }

        return messages.get(type, 'No existe el mensaje "{}"'.format(message_number))

    #############################################################
    # Crear archivo de configuración por DEFAULT (Si no existe) #
    def create_default_config(self, config_path):
        with open(config_path, 'w', encoding = 'UTF-8') as data:
            data.write('[SAP_CONNECTION]\n')
            data.write('ashost = 10.201.2.59\n')
            data.write('sysnr = 00\n') 
            data.write('client = 100\n') 
            data.write('saprouter =\n') 
            data.write('user = autocontab\n') 
            data.write('pass = Autocontabs.5\n\n')
            data.write('[DB_CONNECTION]\n')
            data.write('server = 10.200.1.7\n')
            data.write('dbname = VAD20\n') 
            data.write('user = sa\n') 
            data.write('pass = \n\n') 
            data.write('[PARAMETERS]\n') 
            data.write('minutes = 10\n')     
            data.write('date = \n')             
        data.close()

    def connect_sql(self, config:cp.ConfigParser):
        # No olvides instalar el driver ODBC de microsoft
        # parametros de conexion SQL
        db_driver = '{ODBC Driver 17 for SQL Server}' # Driver sql server
        server = config['DB_CONNECTION']['SERVER']
        dbname = config['DB_CONNECTION']['DBNAME']
        usr = config['DB_CONNECTION']['USER']
        pwd = config['DB_CONNECTION']['PASS']
        con_str = f"DRIVER={db_driver};SERVER={server};DATABASE={dbname};UID={usr};PWD={pwd};"
        
        dbcon = None
        try:
            dbcon = db.connect(con_str)
            
        except db.InterfaceError as e: #Logon error
            message_array = e.args[1].split(';')
            print(message_array[0])
            if self.exec_ui:
                messagebox.showerror(title = "Error de conexión SQL", message = message_array[0])                
        
        except db.OperationalError as e:
            message_array = e.args[1].split(';')            
            print(message_array[0])
            if self.exec_ui:
                messagebox.showerror(title = "Error de conexión SQL", message = message_array[0])            
                
        return dbcon

    
    def connect_sap(self, config:cp.ConfigParser):
        sapcon = None
        try:
            sapcon = sap.Connection(
                ashost=config['SAP_CONNECTION']['ASHOST'], 
                sysnr =config['SAP_CONNECTION']['SYSNR'], 
                client=config['SAP_CONNECTION']['CLIENT'], 
                user  =config['SAP_CONNECTION']['USER'], 
                passwd=config['SAP_CONNECTION']['PASS'], 
                router=config['SAP_CONNECTION']['SAPROUTER']
            )

        except sap.CommunicationError as e: #Logon error
            print(e.message)
            if self.exec_ui:
                messagebox.showerror(title = "Error de conexión SAP", message = e.message)  

        except sap.LogonError as e: #Logon error
            print(e.message)
            if self.exec_ui:
                messagebox.showerror(title = "Error de conexión SAP", message = e.message)  

        except sap._exception.ExternalRuntimeError as e:
            print(e.message)
            if self.exec_ui:
                messagebox.showerror(title = "Error de conexión SAP", message = e.message)  

        return sapcon

    def read_config(self):   
        print(CONFIG_PATH)     
        config = cp.ConfigParser()
        config.read(CONFIG_PATH)
        return config
    
    def get_config(self):
        return self.config    
    
    ###########################################
    # Escribir log de mensajes de la aplición #
    def write_log(self, log_path, lines, exec_ui = False): 
        array_date = str(dt.date.today()).split('-')
        file = "LOG_{}-{}-{}.txt".format(array_date[2], array_date[1], array_date[0])
        if exec_ui:
            file = "UILOG_{}-{}-{}.txt".format(array_date[2], array_date[1], array_date[0])

        with open(log_path + file, 'a', encoding='UTF-8') as log:
            log.write(lines+'\n')
        log.close()

    ###############################################################
    # Crear directorios necesarios para la ejecución del programa #
    def create_missing_directories(self):
        if not os.path.isfile(CONFIG_PATH):
            self.create_default_config(CONFIG_PATH)

        if not os.path.exists(LOG_PATH): # Verificar que el dir applog exista; si no se crea
            os.makedirs(LOG_PATH)

    def convert_string_to_date(self, date:str):
        dateArray = date.split('-')
        return dt.date(int(dateArray[2]), int(dateArray[1]), int(dateArray[0]))
    
    def convert_to_datetime(self, date, time):
        dateArray = date.strftime(REVERSE_DATE_FORMAT).split('-')
        timeArray = time.strftime(TIME_FORMAT).split(':')
        return dt.datetime(int(dateArray[0]), int(dateArray[1]), int(dateArray[2]), int(timeArray[0]), int(timeArray[1]), int(timeArray[2]))

    def execute_query(self, db_fields = '*', db_table = '', db_where = '', db_order = '', db_group = ''):
        db_query = f"SELECT {db_fields} FROM {db_table} WHERE {db_where} ORDER BY {db_order}"
        if db_group:
            db_query = f"SELECT {db_fields} FROM {db_table} WHERE {db_where} GROUP BY {db_group} ORDER BY {db_order}"
        
        while True:
            try:
                cursor = self.dbcon.cursor()
                result = cursor.execute(db_query)
                
            except db.InterfaceError as e: #Logon error
                message_array = e.args[1].split(';')
                print(message_array[0])
                if self.exec_ui:
                    messagebox.showerror(title = "Error SQL", message = message_array[0])                
            
            except db.OperationalError as e:
                message_array = e.args[1].split(';')
                print(message_array[0])
                if self.exec_ui:
                    messagebox.showerror(title = "Error SQL", message = message_array[0])         

            except AttributeError as e:
                print(self.text_message('e002'))
                if self.exec_ui:
                    messagebox.showerror(title = "Error SQL", message = self.text_message('e002'))                         
            else:
                break

            for i in range(1, 11):
                print(self.text_message('w003').format(i))
                time.sleep(1)

            print(self.text_message('w004'))                  
            self.dbcon = self.connect_sql(self.get_config())

        return result

    def get_table_ma_transaccion(self, date:dt.date, center=''):
        db_fields = "id, c_localidad, c_numero, c_caja, f_fecha, h_hora, cod_principal, cantidad, precio, subtotal, impuesto, total, descuento"
        db_table = "[vad20].[dbo].[ma_transaccion]"
        db_where = f"f_fecha = '{date.strftime(SIMPLE_DATE_FORMAT)}'"
        if center:
            db_where = f"f_fecha = '{date.strftime(SIMPLE_DATE_FORMAT)}' AND c_localidad = '{center}'"

        db_order = "c_localidad"
        cursor = self.execute_query(db_fields, db_table, db_where, db_order)

        index = 0
        mandt = self.get_config()['SAP_CONNECTION']['CLIENT']
        self.t_ma_transaccion = []        
        for row in cursor:            
            self.t_ma_transaccion.append({
                'MANDT': mandt,
                'ID': str(row[0]),
                'C_LOCALIDAD': str(row[1])[2:],
                'C_NUMERO': row[2],
                'C_CAJA': row[3],
                'F_FECHA': row[4],
                'H_HORA': row[5],
                'COD_PRINCIPAL': row[6],
                'CANTIDAD': row[7],
                'PRECIO': row[8],
                'SUBTOTAL': row[9],
                'IMPUESTO': row[10],
                'TOTAL': row[11],
                'DESCUENTO': row[12]
            })

            self.t_ma_transaccion[index]['F_FECHA'] = row[4].strftime(SIMPLE_DATE_FORMAT)
            self.t_ma_transaccion[index]['H_HORA'] = row[5].strftime(TIME_FORMAT)

            index += 1 

        return self.t_ma_transaccion
    
    def call_remote_function(self, werks, date, t_ma_transaccion):
        result = 4
        while True:
            try:
                if not self.sapcon is None:
                    result = self.sapcon.call(REMOTE_FUNCTION,
                                        I_WERKS = werks,
                                        I_BUDAT = date,
                                        T_TRANSACCIONES = t_ma_transaccion
                                        ) # Invocar RFC
                
            except sap.CommunicationError as e:
                print(e.message)
                if self.exec_ui:
                    messagebox.showerror(title = "Error SAP", message = e.message)
                self.sapcon = None

            except sap.LogonError as e:
                print(e.message)
                if self.exec_ui:
                    messagebox.showerror(title = "Error SAP", message = e.message)
                self.sapcon = None

            except sap._exception.ExternalRuntimeError as e:
                print(e.message)
                if self.exec_ui:
                    messagebox.showerror(title = "Error SAP", message = e.message)
                self.sapcon = None

            except sap._exception.ABAPRuntimeError as e:
                print(e.message)
                if self.exec_ui:
                    messagebox.showerror(title = "Error SAP", message = e.message)
                self.sapcon = None
            else: 
                if not self.sapcon is None:
                    break
            
            while self.sapcon == None:
                for i in range(1, 11):
                    print(self.text_message('w001').format(i))
                    time.sleep(1)

                print(self.text_message('w002'))
                self.sapcon = self.connect_sap(self.get_config())

        return { 'E_SUBRC': result['E_SUBRC'], 'E_MSG': result['E_MSG'] }

    def syncronize(self, date, center = '', exec_ui = False):

        r_data = {
            'msg': '',
            '//TRANSACCIONES': 0,
        }

        self.exec_ui = exec_ui      
        
        self.date = dt.datetime.today().strftime(LOG_DATETIME_FORMAT) # public attribute   

        if center:
            center = f"TD{center}"

        if type(self.dbcon) != db.Connection:
            self.dbcon = self.connect_sql(self.get_config())

        if type(self.sapcon) != sap.Connection:
            self.sapcon = self.connect_sap(self.get_config())

        t_ma_transaccion = self.get_table_ma_transaccion(date, center)

        # No hay registros en ma_transaccion
        if len(t_ma_transaccion) == 0:
            print('\n'+ self.text_message('e001'))
            self.write_log(LOG_PATH, str(self.date) + '\n' + self.text_message('e001') + '\n', exec_ui)
            return { '-': self.text_message('e001') }
 
        # Invocación de la RFC en SAP
        self.write_log(LOG_PATH, '\n'+ str(self.date), exec_ui)
        
        result_code = self.call_remote_function(center[2:], 
                                                date.strftime(SIMPLE_DATE_FORMAT), 
                                                t_ma_transaccion
                                            )
        
        if result_code['E_SUBRC'] == 0:
            if exec_ui:
                r_data['//TRANSACCIONES'] = len(t_ma_transaccion)
                self.write_log(LOG_PATH, f'\n//TRANSACCIONES {date.strftime(DATE_FORMAT)} \n--Cant registros: {len(t_ma_transaccion)}', exec_ui)                    

                r_data['msg'] = result_code["E_MSG"]

            else:
                r_data = []

                print(f'\n//TRANSACCIONES ({date.strftime(DATE_FORMAT)}) \n--Cant registros: {len(t_ma_transaccion)}')
                self.write_log(LOG_PATH, f'\n//TRANSACCIONES {date.strftime(DATE_FORMAT)} \n--Cant registros: {len(t_ma_transaccion)}', exec_ui)

                print(f'\n{result_code["E_MSG"]}')

            self.write_log(LOG_PATH, f'\n{result_code["E_MSG"]}', exec_ui)
        else:            
            self.write_log(LOG_PATH, f"\n{self.text_message('e003').format(result_code['E_SUBRC'], result_code['E_MSG'])}\n", exec_ui) # Error al enviar a SAP

        return r_data

if __name__ == '__main__':
  
    df = DetalleFactura()
    while True:
        os.system('cls')

        print('\n'+df.text_message('i001').format(dt.datetime.today().strftime(LOG_DATETIME_FORMAT)))       
        
        date = dt.datetime.today()
        if df.get_config()['PARAMETERS']['date'] != '':
            date = df.convert_string_to_date(df.get_config()['PARAMETERS']['date'])     
        
        df.syncronize(date=date)
        
        print(df.text_message('i002').format(dt.datetime.today().strftime(LOG_DATETIME_FORMAT)))

        time.sleep(int(df.get_config()['PARAMETERS']['minutes']) * 60)