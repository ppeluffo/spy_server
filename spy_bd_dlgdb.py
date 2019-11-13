#!/usr/bin/python3 -u
"""
Modulo de trabajo con la BD GDA
"""

from sqlalchemy import create_engine
from sqlalchemy import text
from spy import Config
from collections import defaultdict
from spy_log import log
import MySQLdb

# ------------------------------------------------------------------------------
class DLGDB:


    def __init__(self, modo='local', server='comms'):

        self.datasource = ''
        self.engine = ''
        self.conn = ''
        self.connected = False
        self.server = server

        if modo == 'spymovil':
            self.url = Config['BDATOS']['url_gda_spymovil']
        elif modo == 'local':
            self.url = Config['BDATOS']['url_gda_local']
        elif modo == 'ute':
            self.url = Config['BDATOS']['url_gda_local']
        return


    def connect(self, tag='DLGDB'):
        """
        Retorna True/False si es posible generar una conexion a la bd GDA
        """
        if self.connected:
            return self.connected

        try:
            self.engine = create_engine(self.url)
        except Exception as err_var:
            self.connected = False
            log(module=__name__, server=self.server, function='connect', msg='ERROR_{}: engine NOT created. ABORT !!'.format(tag))
            log(module=__name__, server=self.server, function='connect', msg='ERROR: EXCEPTION_{0} {1}'.format(tag, err_var))
            exit(1)

        try:
            self.conn = self.engine.connect()
            self.connected = True
        except Exception as err_var:
            self.connected = False
            log(module=__name__, server=self.server, function='connect', msg='ERROR_{}: NOT connected. ABORT !!'.format(tag))
            log(module=__name__, server=self.server, function='connect', msg='ERROR: EXCEPTION_{0} {1}'.format(tag, err_var))
            exit(1)

        return self.connected


    def read_dlg_conf(self, dlgid, tag='DLGDB'):
        '''
        Leo la configuracion desde DLGDB
                +----------+---------------+------------------------+----------+
                | canal    | parametro     | value                  | param_id |
                +----------+---------------+------------------------+----------+
                | BASE     | RESET         | 0                      |      899 |
                | BASE     | UID           | 304632333433180f000500 |      899 |
                | BASE     | TPOLL         | 60                     |      899 |
                | BASE     | COMMITED_CONF |                        |      899 |
                | BASE     | IMEI          | 860585004331632        |      899 |

                EL diccionario lo manejo con 2 claves para poder usar el metodo get y tener
                un valor por default en caso de que no tenga alguna clave
        '''
        log(module=__name__, server=self.server, function='read_dlg_conf', dlgid=dlgid, level='SELECT', msg='start_{}'.format(tag))

        if not self.connect(tag):
            log(module=__name__, server=self.server, function='read_dlg_conf', dlgid=dlgid, msg='ERROR_{}: can\'t connect !!'.format(tag))
            return

        sql = "SELECT magName, tbMCol, disp FROM tbDlgParserConf WHERE dlgId = '{}'".format (dlgid)
        try:
            query = text(sql)
        except Exception as err_var:
            log(module=__name__, server=self.server, function='read_dlg_conf', dlgid=dlgid, msg='ERROR_{0}: SQLQUERY: {1}'.format(tag, sql))
            log(module=__name__, server=self.server, function='read_dlg_conf', dlgid=dlgid, msg='ERROR_{0}: EXCEPTION {1}'.format(tag, err_var))
            return False

        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, server=self.server, function='read_dlg_conf', dlgid=dlgid,msg='ERROR_{}: exec EXCEPTION {}'.format(tag, err_var))
            return False

        results = rp.fetchall()
        d = defaultdict(dict)
        for row in results:
            mag_name, tbm_col, disp = row
            d[mag_name] = ( tbm_col, disp,)
            log(module=__name__, server=self.server, function='read_dlg_conf', dlgid=dlgid, level='SELECT', msg='BD_{0} conf: [{1}][{2}][{3}]'.format( tag, mag_name, tbm_col, disp))

        return d


    def insert_data_line(self, dlgid, d, tag='DLGDB'):
        '''
        En este caso (dlgdb, UTE), el d tiene otros 2 d dentro, d[DATA], d[CONF]
        Inserto en las 3 tablas. datos, online, dinama

        '''
        if not self.connect():
            log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid, msg='ERROR_{}: can\'t connect!!'.format(tag))
            exit(0)

        d_conf = d['CONF']
        d_data = d['DATA']

        data = list()
        for mag_name in d_data:
            if mag_name == 'timestamp' or mag_name == 'RCVDLINE':
                continue
            col, disp = d_conf[mag_name]
            val = d_data[mag_name]
            data.insert( (mag_name,val,col,disp,))

        # Tengo c/elemento en una lista por lo que puedo acceder ordenadamente a la secuencia.
        # Armo el insert.
        sql_main = 'INSERT INTO tbMain (dlgId, fechaHoraData, fechaHoraSys '
        sql_online = 'INSERT INTO tbMainOnline (dlgId, fechaHoraData, fechaHoraSys '
        sql_dinama = 'INSERT INTO tbMain_auxDinama (dlgId, fechaHoraData, fechaHoraSys '
        # Variables:
        for ( mag_name,val,col,disp ) in data:
            sql_main += 'mag{},disp{} '.format(col)
            sql_online += 'mag{},disp{} '.format(col)
            sql_dinama += 'mag{},disp{} '.format(col)

        # Valores
        sql_main += ') VALUES ( {0},{1},now() '.format(dlgid, d_data['timestamp'])
        sql_online += ') VALUES ( {0},{1},now() '.format(dlgid, d_data['timestamp'])
        sql_dinama += ') VALUES ( {0},{1},now() '.format(dlgid, d_data['timestamp'])

        for ( mag_name,val,col,disp ) in data:
            sql_main += '{},{} '.format(val,disp)
            sql_online += '{},{} '.format(val, disp)
            sql_dinama += '{},{} '.format(val, disp)

        # Tail
        sql_main += ')'
        sql_online += ')'
        sql_dinama += ')'

        print(sql_main)
        return True

        errors = 0

        # main
        try:
            query = text(sql_main)
        except Exception as err_var:
            log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid, msg='ERROR_{0}: SQLQUERY: {1}'.format(tag, sql_main))
            log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid, msg='ERROR_{0}: EXCEPTION {1}'.format(tag, err_var))

        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            if 'Duplicate entry' in str(err_var):
                # Los duplicados no hacen nada malo. Se da mucho en testing.
                log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid, msg='WARN_{}: Duplicated Key'.format(tag))
            else:
                log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid,msg='ERROR_{}: exec EXCEPTION {}'.format(tag, err_var))

        # online
        try:
            query = text(sql_online)
        except Exception as err_var:
            log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid, msg='ERROR_{0}: SQLQUERY: {1}'.format(tag, sql_online))
            log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid, msg='ERROR_{0}: EXCEPTION {1}'.format(tag, err_var))

        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            if 'Duplicate entry' in str(err_var):
                # Los duplicados no hacen nada malo. Se da mucho en testing.
                log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid, msg='WARN_{}: Duplicated Key'.format(tag))
            else:
                log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid,msg='ERROR_{}: exec EXCEPTION {}'.format(tag, err_var))

        # dinama
        try:
            query = text(sql_dinama)
        except Exception as err_var:
            log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid, msg='ERROR_{0}: SQLQUERY: {1}'.format(tag, sql_dinama))
            log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid, msg='ERROR_{0}: EXCEPTION {1}'.format(tag, err_var))

        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            if 'Duplicate entry' in str(err_var):
                # Los duplicados no hacen nada malo. Se da mucho en testing.
                log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid, msg='WARN_{}: Duplicated Key'.format(tag))
            else:
                log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid,msg='ERROR_{}: exec EXCEPTION {}'.format(tag, err_var))

        if errors > 0:
            return False
        else:
            return True


