#!/usr/bin/python3 -u
# -*- coding: utf-8 -*-
"""
Modulo de trabajo con la BD OSE
"""

from sqlalchemy import create_engine
from sqlalchemy import text
from spy import Config
from collections import defaultdict
import re
from spy_log import log

# ------------------------------------------------------------------------------
class BDOSE_PQ:

    def __init__(self, modo='local'):

        self.datasource = ''
        self.engine = ''
        self.conn = ''
        self.connected = False

        if modo == 'spymovil':
            self.url = Config['BDATOS']['url_bdose_spymovil']
        elif modo == 'local':
            self.url = Config['BDATOS']['url_bdose_local']
        return


    def connect(self, tag='PQ'):
        """
        Retorna True/False si es posible generar una conexion a la bd OSE
        """
        if self.connected:
            return self.connected

        try:
            self.engine = create_engine(self.url)
        except Exception as err_var:
            self.connected = False
            log(module=__name__, function='connect', msg='ERROR: BDOSE {0} engine NOT created. ABORT !!'.format(tag))
            log(module=__name__, function='connect', msg='EXCEPTION {}'.format(err_var))
            exit(1)

        try:
            self.conn = self.engine.connect()
            self.connected = True
        except Exception as err_var:
            self.connected = False
            log(module=__name__, function='connect', msg='ERROR: BDOSE {0} NOT connected. ABORT !!'.format(tag))
            log(module=__name__, function='connect', msg='EXCEPTION {}'.format(err_var))
            exit(1)

        return self.connected


    def read_dlg_conf(self, dlgid, tag='PQ'):

        log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, level='SELECT', msg='start')

        if not self.connect():
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='ERROR: BDOSE {0} can\'t connect !!'.format(tag))
            return

        query = text("""SELECT timerPoll, timerDial,c0_name, c0_eRange, c0_minValue,c0_maxValue,c0_mag,\
                    c1_name,c1_eRange,c1_minValue,c1_maxValue,c1_mag,c2_name,c2_eRange,c2_minValue,c2_maxValue,c2_mag,\
                    c3_name,c3_eRange,c3_minValue,c3_maxValue,c3_mag,c4_name,c4_eRange,c4_minValue,c4_maxValue,c4_mag,\
                    consigna_mode,consigna_hhmm1,consigna_hhmm2,pwrSaveModo,pwrSaveStartTime,pwrSaveEndTime FROM PQ_tbUnidades WHERE dlgid = '%s'""" % (dlgid))
        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='ERROR: BDOSE can\'t exec {0} !!'.format(tag))
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            return

        result = rp.first()
        d = defaultdict(dict)
        d[('BASE', 'TPOLL')] = result[0]
        d[('BASE', 'TDIAL')] = result[1]
        # Canales analogicos: C0=>A0,C1=>A1, C2=>A2
        # CO=>A0
        d[('A0', 'NAME')] = result[2]
        # el imin,imax vienen el Ax_Range con el formato 0-20mA. Debo parsear.
        try:
            imin, imax, *r = re.split('-|mA', result[3])
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='ERROR: split {}'.format(result[3]))
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            imin, imax = 0, 0

        d[('A0', 'IMIN')] = imin
        d[('A0', 'IMAX')] = imax
        d[('A0', 'MMIN')] = result[4]
        d[('A0', 'MMAX')] = result[5]
        # C1=>A1
        d[('A1', 'NAME')] = result[7]
        try:
            imin, imax, *r = re.split('-|mA', result[8])
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='ERROR: split {}'.format(result[8]))
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            imin, imax = 0, 0

        d[('A1', 'IMIN')] = imin
        d[('A1', 'IMAX')] = imax
        d[('A1', 'MMIN')] = result[9]
        d[('A1', 'MMAX')] = result[10]
        # C2=>A2
        d[('A2', 'NAME')] = result[12]
        try:
            imin, imax, *r = re.split('-|mA', result[13])
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='ERROR: split {}'.format(result[13]))
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            imin, imax = 0, 0
        d[('A2', 'IMIN')] = imin
        d[('A2', 'IMAX')] = imax
        d[('A2', 'MMIN')] = result[14]
        d[('A2', 'MMAX')] = result[15]

        # Los canales C3 y C4 corresponden a contadores que pueden traer caudal
        # C3 => C0
        d[('C0', 'NAME')] = result[17]
        try:
           r, magpp = re.split('-', result[18])
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='ERROR: split {}'.format(result[18]))
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            magpp = 0
        d[('C0', 'MAGPP')] = magpp
        d[('C0', 'PERIOD')] = 100       # Campos de relleno
        d[('C0', 'PWIDTH')] = 100
        d[('C0', 'SPEED')] = 'LS'

        # C4 => C1
        d[('C1', 'NAME')] = result[22]
        try:
            r, magpp = re.split('-', result[23])
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='ERROR: split {}'.format(result[23]))
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            magpp = 0
        d[('C1', 'MAGPP')] = magpp
        d[('C1', 'PERIOD')] = 100       # Campos de relleno
        d[('C1', 'PWIDTH')] = 100
        d[('C1', 'SPEED')] = 'LS'

        # CONSIGNAS:
        # PQ no tiene el parametro DOUTPUTS por lo cual debo ponerlo por default
        # Hay que adecuar los formatos de HHMM
        cons_enabled = result[27]
        if cons_enabled == 0:   # disabled
            d[('DOUTPUTS', 'MODO')] = 'OFF'
        else:
            d[('DOUTPUTS', 'MODO')] = 'CONS'

        try:
            hh, mm, *r = re.split(':', str(result[28]))
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='ERROR: split {}'.format(result[28]))
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            hh, mm = 0, 0
        d[('CONS', 'HHMM1')] = '{0}{1}'.format(hh, mm)

        try:
            hh, mm, *r = re.split(':', str(result[29]))
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='ERROR: split {}'.format(result[29]))
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            hh, mm = 0, 0
        d[('CONS', 'HHMM2')] = '{0}{1}'.format(hh, mm)

        # PWRSAVE
        # BDOSE guarda pwr_save como 0(off) o 1(on). No lo convierto aqui. Lo hago en base.
        pwr_save_modo = result[30]
        d[('BASE', 'PWRS_MODO')] = pwr_save_modo
        # result15,16 son timedelta por lo tanto los separo en su representacion str.
        # split me devuelve strings por lo que uso %s%s.
        try:
            hh, mm, *r = re.split(':', str(result[31]))
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='ERROR: split {}'.format(result[31]))
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            hh, mm = 0, 0
        d[('BASE', 'PWRS_HHMM1')] = '{0}{1}'.format (hh, mm)

        try:
            hh, mm, *r = re.split(':', str(result[32]))
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='ERROR: split {}'.format(result[32]))
            log(module=__name__, function='read_dlg_conf PQ', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            hh, mm = 0, 0
        d[('BASE', 'PWRS_HHMM2')] = '{0}{1}'.format (hh, mm)

        return (d)


    def update(self, dlgid, d, tag='PQ'):

        log(module=__name__, function='update', dlgid=dlgid, level='SELECT', msg='start {}'.format(tag))

        if self.connect() == False:
            log(module=__name__, function='update', dlgid=dlgid, msg='ERROR: BDOSE {0}can\'t connect !!'.format(tag))
            return False

        # PASS1: Inserto frame en la tabla de INITS.
        query = text("INSERT INTO {0}_tbInits (dlgId,fechaHora,rcvdFrame,sqe ) VALUES ( '{1}',NOW(),'{2}','{3}' )".format\
                    (tag, dlgid, d['RCVDLINE'],d['CSQ']))
        try:
            self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='update', dlgid=dlgid, msg='ERROR: BDOSE can\'t exec {0} !!'.format(tag))
            log(module=__name__, function='update', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            return False

        # PASS2: Actualizo los parametros dinamicos
        query = text("UPDATE {0}_tbUnidades SET version='{1}', imei='{2}', commitedConf=0, ipAddress='{3}' WHERE dlgid='{4}'".format\
                    (tag, d['FIRMWARE'], d['IMEI'], d['IPADDRESS'], dlgid))
        try:
            self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='update', dlgid=dlgid, msg='ERROR: BDOSE can\'t exec {0} !!'.format(tag))
            log(module=__name__, function='update', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            return False

        return True


    def process_commited_conf(self, dlgid, tag='PQ'):

        log(module=__name__, function='process_commited_conf', dlgid=dlgid, level='SELECT', msg='start {}'.format(tag))

        if not self.connect():
            log(module=__name__, function='process_commited_conf', dlgid=dlgid, msg='ERROR: BDOSE {0} can\'t connect !!'.format(tag))
            return False

        query = text("SELECT commitedConf FROM {0}_tbUnidades WHERE dlgId = '{1}'".format(tag, dlgid))
        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='process_commited_conf', dlgid=dlgid, msg='ERROR: BDOSE can\'t exec {0} !!'.format(tag))
            log(module=__name__, function='process_commited_conf', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            return

        row = rp.first()
        cc, *rid = row
        log(module=__name__, function='process_commited_conf', dlgid=dlgid, level='SELECT', msg='cc={}'.format(cc))
        return cc


    def clear_commited_conf(self, dlgid, tag='PQ'):

        log(module=__name__, function='clear_commited_conf', dlgid=dlgid, level='SELECT', msg='start {}'.format(tag))

        if not self.connect():
            log(module=__name__, function='clear_commited_conf', dlgid=dlgid, msg='ERROR: BDOSE {0}can\'t connect !!'.format(tag))
            return False

        query = text("UPDATE {0}_tbUnidades SET commitedConf = '0' WHERE dlgId = '{1}'".format(tag, dlgid ))
        print ( query)
        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='clear_commited_conf', dlgid=dlgid, msg='ERROR: BDOSE can\'t exec {0} !!'.format(tag))
            log(module=__name__, function='clear_commited_conf', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            return

        return


    def insert_data_line(self, dlgid, d, tag='PQ'):

        if not self.connect():
            log(module=__name__, function='insert_data_line', dlgid=dlgid, msg='ERROR: BDOSE {0} can\'t connect !!'.format(tag))
            return False

        query = text("""INSERT INTO PQ_tbDatos ( dlgId,fechaSys,fechaData,pkMonitoreo,pkInstalacion,c0_name,c0_value,\
         c1_name,c1_value,c2_name,c2_value,c5_name,c5_value, rcvdFrame ) VALUES ( '{0}', now(), '{1}',\
         (select pkMonitoreo from PQ_tbInstalaciones where dlgId='{0}}' and status='ACTIVA'),\
         (select pkInstalacion from PQ_tbInstalaciones where dlgId='{0}' and status='ACTIVA'),'pA','{2}','pB','{3}','q0','{4}','bt','{5}','{6}'\
         )""".format(dlgid, d.get('timestamp', '00-00-00 00:00'), d.get('pA', '0'), d.get('pB', '0'), d.get('bt', '0'),
                     d.get('RCVDLINE', 'err')))

        try:
            self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='insert_data_line', dlgid=dlgid, msg='QUERY {}'.format(query))
            log(module=__name__, function='insert_data_line', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))

        return


    def insert_data_online(self, dlgid, d, tag='PQ'):

        if not self.connect():
            log(module=__name__, function='insert_data_online', dlgid=dlgid, msg='ERROR: can\'t connect {0} !!'.format(tag))
            return

        query = text("""INSERT INTO PQ_tbDatos ( dlgId,fechaSys,fechaData,pkMonitoreo,pkInstalacion,c0_name,c0_value,\
        c1_name,c1_value,c2_name,c2_value,c5_name,c5_value, sqe) VALUES ( '{0}', now(), '{1}',\
        (select pkMonitoreo from PQ_tbInstalaciones where dlgId='{0}}' and status='ACTIVA'),\
        (select pkInstalacion from PQ_tbInstalaciones where dlgId='{0}' and status='ACTIVA'),'pA','{2}','pB','{3}','q0','{4}','bt','{5}',\
        ( select sqe from PQ_tbInits where dlgId='{0}' order by pkInits DESC limit 1)
        )""".format(dlgid, d.get('timestamp', '00-00-00 00:00'),d.get('pA', '0'),d.get('pB', '0'),d.get('bt', '0')  ))

        try:
            self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='insert_data_online', dlgid=dlgid, msg='QUERY {}'.format(query))
            log(module=__name__, function='insert_data_online', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))

        delquery = text("""DELETE FROM PQ_tbDatosOnline WHERE dlgId = '{0}' AND  pkDatos NOT IN ( SELECT * FROM ( SELECT pkDatos FROM PQ_tbDatosOnline WHERE dlgId = '{0}' \
        ORDER BY fechaData DESC LIMIT 1) AS temp )""".format(dlgid))
        try:
            self.conn.execute(delquery)
        except Exception as err_var:
            log(module=__name__, function='insert_data_online', dlgid=dlgid,msg='QUERY: DELETE {}'.format(query))
            log(module=__name__, function='insert_data_online', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))

        return


class BDOSE_PZ (BDOSE_PQ):

    def connect(self, tag='PZ'):
        return BDOSE_PQ.connect(self, tag=tag )


    def read_dlg_conf(self, dlgid, tag='PZ'):
        #Los pozos deben tener predido el DIST=ON solamente.
        log(module=__name__, function='read_dlg_conf PZ', dlgid=dlgid, level='SELECT', msg='start')

        if not self.connect():
            log(module=__name__, function='read_dlg_conf PZ', dlgid=dlgid, msg='ERROR: can\'t connect {0} !!'.format(tag))
            return

        query = text("SELECT timerPoll FROM PZ_tbUnidades WHERE dlgid = '{0}'".format(dlgid))
        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf PZ', dlgid=dlgid, msg='ERROR: BDOSE can\'t exec PZ !!')
            log(module=__name__, function='read_dlg_conf PZ', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            return

        result = rp.first()
        d = defaultdict(dict)
        d[('BASE', 'DIST')] = 1
        d[('BASE', 'TPOLL')] = result[0]
        return d


    def update(self, dlgid, d, tag='PZ'):
        return BDOSE_PQ.update(self, dlgid, d, tag=tag)


    def process_commited_conf(self, dlgid, tag='PZ'):
        return BDOSE_PQ.process_commited_conf(self, dlgid, tag=tag)


    def clear_commited_conf(self, dlgid, tag='PZ'):
        return BDOSE_PQ.clear_commited_conf(self, dlgid, tag=tag)


    def insert_data_line(self, dlgid, d, tag='PZ'):

        if not self.connect():
            log(module=__name__, function='insert_data_line', dlgid=dlgid, msg='ERROR: can\'t connect {0} !!'.format(tag))
            exit(0)

        query = text("""INSERT IGNORE INTO PZ_tbDatos (pozoId,fechaSys,fechaData,pkMonitoreo,pkInstalacion,c0_name,c0_value, rcvdFrame) \
        	        VALUES ( '{0}', now(), '{1}', (select pkMonitoreo from PZ_tbInstalaciones where pozoId='{0}' and status='ACTIVA'), \
        	        (select pkInstalacion from PZ_tbInstalaciones where pozoId='{0}' and status='ACTIVA'),'H1', {2}, '{3}' \
        	        )""".format(dlgid, d.get('timestamp', '00-00-00 00:00'), d.get('DIST', '-1'), d.get('RCVDLINE', 'err')))

        try:
            self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='insert_data_line', dlgid=dlgid, msg='QUERY {}'.format(query))
            log(module=__name__, function='insert_data_line', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))

        return


    def insert_data_online(self, dlgid, d, tag='PZ'):

        if not self.connect():
            log(module=__name__, function='insert_data_line', dlgid=dlgid, msg='ERROR: can\'t connect {0} !!'.format(tag))
            exit(0)

        query = text("""INSERT IGNORE INTO PZ_tbDatosOnline (pozoId,fechaSys,fechaData,pkMonitoreo,pkInstalacion,c0_name,c0_value, sqe) \
         	        VALUES ( '{0}', now(), '{1}', (select pkMonitoreo from PZ_tbInstalaciones where pozoId='{0}' and status='ACTIVA'), \
         	        (select pkInstalacion from PZ_tbInstalaciones where pozoId='{0}' and status='ACTIVA'),'H1', {2}, '{3}',\
         	        ( select sqe from PZ_tbInits where dlgId='{0}}' order by pkInits DESC limit 1)
         	        )""".format(dlgid, d.get('timestamp', '00-00-00 00:00'), d.get('DIST', '-1')))

        try:
            self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='insert_data_line', dlgid=dlgid, msg='QUERY {}'.format(query))
            log(module=__name__, function='insert_data_line', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))

        delquery = text("""DELETE FROM PZ_tbDatosOnline WHERE pozoId = '{0}' AND  pkDatos NOT IN ( SELECT * FROM ( SELECT pkDatos FROM PZ_tbDatosOnline WHERE pozoId = '{0}' \
        ORDER BY fechaData DESC LIMIT 1) AS temp )""".format(dlgid))
        try:
            self.conn.execute(delquery)
        except Exception as err_var:
            log(module=__name__, function='insert_data_online', dlgid=dlgid,msg='QUERY: DELETE {}'.format(query))
            log(module=__name__, function='insert_data_online', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))

        return


class BDOSE_TQ (BDOSE_PQ):


    def connect(self, tag='TQ'):
        return BDOSE_PQ.connect(self, tag=tag )


    def read_dlg_conf(self, dlgid, tag='TQ'):

        log(module=__name__, function='read_dlg_conf TQ', dlgid=dlgid, level='SELECT', msg='start')

        if not self.connect():
            log(module=__name__, function='read_dlg_conf TQ', dlgid=dlgid, msg='ERROR: can\'t connect {0} !!'.format(tag))
            return

        query = text("""SELECT timerPoll,timerDial,c0_name,c0_eRange,c0_minValue,c0_maxValue,c1_name,c1_eRange,\
                      c1_minValue,c1_maxValue, c2_name,c2_eRange,c2_minValue,c2_maxValue, pwrSaveModo,pwrSaveStartTime,\
                      pwrSaveEndTime FROM TQ_tbUnidades WHERE dlgid = '%s'""" % (dlgid))

        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf TQ', dlgid=dlgid, msg='ERROR: can\'t exec {0} !!'.format(tag))
            log(module=__name__, function='read_dlg_conf TQ', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))
            return

        result = rp.first()
        d = defaultdict(dict)
        d[('BASE', 'TPOLL')] = result[0]
        d[('BASE', 'TDIAL')] = result[1]
        # Canales analogicos: C0=>A0,C1=>A1, C2=>A2
        # CO=>A0
        d[('A0', 'NAME')] = result[2]
        try:
            imin, imax, *r = re.split('-|mA', result[3])
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf TQ', dlgid=dlgid, msg='ERROR: split {}'.format(result[3]))
            log(module=__name__, function='read_dlg_conf TQ', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            imin, imax = 0, 0

        d[('A0', 'IMIN')] = imin
        d[('A0', 'IMAX')] = imax
        d[('A0', 'MMIN')] = result[4]
        d[('A0', 'MMAX')] = result[5]

        # C1=>A1
        d[('A1', 'NAME')] = result[6]
        try:
            imin, imax, *r = re.split('-|mA', result[7])
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf TQ', dlgid=dlgid, msg='ERROR: split {}'.format(result[7]))
            log(module=__name__, function='read_dlg_conf TQ', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            imin, imax = 0, 0

        d[('A1', 'IMIN')] = imin
        d[('A1', 'IMAX')] = imax
        d[('A1', 'MMIN')] = result[8]
        d[('A1', 'MMAX')] = result[9]

        # C2=>A2
        d[('A2', 'NAME')] = result[10]
        try:
            imin, imax, *r = re.split('-|mA', result[11])
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf TQ', dlgid=dlgid, msg='ERROR: split {}'.format(result[11]))
            log(module=__name__, function='read_dlg_conf TQ', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            imin, imax = 0, 0

        d[('A2', 'IMIN')] = imin
        d[('A2', 'IMAX')] = imax
        d[('A2', 'MMIN')] = result[12]
        d[('A2', 'MMAX')] = result[13]

        # BDOSE guarda pwr_save como 0(off) o 1(on). No lo convierto aqui. Lo hago en base.
        pwr_save_modo = result[14]
        d[('BASE', 'PWRS_MODO')] = pwr_save_modo

        # result15,16 son timedelta por lo tanto los separo en su representacion str.
        # split me devuelve strings por lo que uso %s%s.
        try:
            hh, mm, *r = re.split(':', str(result[15]))
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf TQ', dlgid=dlgid, msg='ERROR: split {}'.format(result[15]))
            log(module=__name__, function='read_dlg_conf TQ', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            hh, mm = 0, 0
        d[('BASE', 'PWRS_HHMM1')] = '{0}{1}'.format (hh, mm)
        try:
            hh, mm, *r = re.split(':', str(result[16]))
        except Exception as err_var:
            log(module=__name__, function='read_dlg_conf TQ', dlgid=dlgid, msg='ERROR: split {}'.format(result[16]))
            log(module=__name__, function='read_dlg_conf TQ', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            hh, mm = 0, 0
        d[('BASE', 'PWRS_HHMM2')] = '{0}{1}'.format (hh, mm)

        for key in d:
            log(module=__name__, function='read_dlg_conf TQ', dlgid=dlgid, msg='DEBUG key={0}, val={1}'.format(key,d[key]))

        return d


    def update(self, dlgid, d, tag='TQ'):
        return BDOSE_PQ.update(self, dlgid, d, tag=tag)


    def process_commited_conf(self, dlgid, tag='TQ'):
        return BDOSE_PQ.process_commited_conf(self, dlgid, tag=tag)


    def clear_commited_conf(self, dlgid, tag='TQ'):
        return BDOSE_PQ.clear_commited_conf(self, dlgid, tag=tag)


    def insert_data_line(self, dlgid, d, tag='TQ'):

        if not self.connect():
            log(module=__name__, function='insert_data_line', dlgid=dlgid, msg='ERROR: can\'t connect {0} !!'.format(tag))
            exit(0)

        query = text("""INSERT IGNORE INTO TQ_tbDatos (tanqueId,fechaSys,fechaData,pkMonitoreo,pkInstalacion,c0_name,c0_value,c1_name,c1_value,rcvdFrame) \
            VALUES ( '{0}', now(), '{1}',(select pkMonitoreo from TQ_tbInstalaciones where tanqueId='{0}' and status='ACTIVA'),\
            (select pkInstalacion from TQ_tbInstalaciones where tanqueId='{0}' and status='ACTIVA'),'H', {2}, 'bt', {3}, '{4}'\
            )""".format( dlgid, d.get('timestamp','00-00-00 00:00'), d.get('H','0'), d.get('bt','0'), d.get('RCVDLINE','err') ) )

        try:
            self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='insert_data_line', dlgid=dlgid, msg='QUERY {}'.format(query))
            log(module=__name__, function='insert_data_line', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))

        return


    def insert_data_online(self, dlgid, d, tag='TQ'):

        if not self.connect():
            log(module=__name__, function='insert_data_line', dlgid=dlgid, msg='ERROR: can\'t connect {0} !!'.format(tag))
            exit(0)

        query = text("""INSERT IGNORE INTO TQ_tbDatosOnline (tanqueId,fechaSys,fechaData,pkMonitoreo,pkInstalacion,c0_name,c0_value,c1_name,c1_value,sqe) \
        VALUES ( '{0}', now(), '{1}', (select pkMonitoreo from TQ_tbInstalaciones where tanqueId='{0}' and status='ACTIVA'),\
        (select pkInstalacion from TQ_tbInstalaciones where tanqueId='{0}' and status='ACTIVA'),'H', {2}, 'bt', {3}, \
        ( select sqe from TQ_tbInits where dlgId='{0}' order by pkInits DESC limit 1)\
        )""".format( dlgid, d.get('timestamp','00-00-00 00:00'), d.get('H','0'), d.get('bt','0') ) )

        try:
            self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='insert_data_line', dlgid=dlgid, msg='QUERY {}'.format(query))
            log(module=__name__, function='insert_data_line', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))

        delquery = text("""DELETE FROM TQ_tbDatosOnline WHERE tanqueId = '{0}' AND  pkDatos NOT IN ( SELECT * FROM ( SELECT pkDatos FROM TQ_tbDatosOnline WHERE tanqueId = '{0}' \
        ORDER BY fechaData DESC LIMIT 1) AS temp )""".format(dlgid))
        try:
            self.conn.execute(delquery)
        except Exception as err_var:
            log(module=__name__, function='insert_data_online', dlgid=dlgid, msg='QUERY: DELETE {}'.format(query))
            log(module=__name__, function='insert_data_online', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))

        return

