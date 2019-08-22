#!/usr/bin/python3 -u
# -*- coding: utf-8 -*-
"""
Modulo de trabajo con la BD OSE
"""

from sqlalchemy import create_engine
from sqlalchemy import text
from spy import Config
import logging
from collections import defaultdict
import re

# Creo un logger local child.
LOG = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
class BDOSE:

    def __init__(self, modo='local'):

        self.datasource = ''
        self.engine = ''
        self.conn = ''
        self.connected = False
        self.group = ''

        if modo == 'spymovil':
            self.url = Config['BDATOS']['url_bdose_spymovil']
        elif modo == 'local':
            self.url = Config['BDATOS']['url_bdose_local']
        return


    def connect(self):
        """
        Retorna True/False si es posible generar una conexion a la bd OSE
        """
        if self.connected == True:
            return (True)

        try:
            self.engine = create_engine(self.url)
        except Exception as errvar:
            self.connected = False
            LOG.info('[{0}] ERROR: engine NOT created {1}.'.format(dlgid, errvar))
            exit(1)

        try:
            self.conn = self.engine.connect()
            self.connected = True
            return (True)
        except Exception as errvar:
            self.connected = False
            LOG.info('ERROR: BDOSE NOT connected {0}.'.format(errvar))
            exit(1)

        return (False)


    def find_group(self, dlgid ):

        if not self.connect():
            LOG.info('[{0}] ERROR: find_group cant connect bdose.'.format(dlgid))
            return False

        # Presiones y caudales ?
        query = text("SELECT pkUnits FROM PQ_tbUnidades WHERE dlgid = '%s'" % (dlgid))
        try:
            rp = self.conn.execute(query)
        except Exception as errvar:
            LOG.info('[{0}] ERROR: find_group(A) exec error {0}.'.format(dlgid, errvar))
            return False

        results = rp.fetchone()
        if results != None:
            self.group = 'PQ'
            LOG.info('[{0}] find_group: PyQ.'.format(dlgid))
            return True

       # Pozos ?
        query = text("SELECT pkUnits FROM PZ_tbUnidades WHERE dlgid = '%s'" % (dlgid))
        try:
            rp = self.conn.execute(query)
        except Exception as errvar:
            LOG.info('[{0}] ERROR: find_group(B) exec error {0}.'.format(dlgid, errvar))
            return False

        results = rp.fetchone()
        if results != None:
            self.group = 'PZ'
            LOG.info('[{0}] find_group: PZ.'.format(dlgid))
            return True

        # Tanques ?
        query = text("SELECT pkUnits FROM TQ_tbUnidades WHERE dlgid = '%s'" % (dlgid))
        try:
            rp = self.conn.execute(query)
        except Exception as errvar:
            LOG.info('[{0}] ERROR: find_group(C) exec error {0}.'.format(dlgid, errvar))
            return False

        results = rp.fetchone()
        if results != None:
            self.group = 'TQ'
            LOG.info('[{0}] find_group: TQ.'.format(dlgid))
            return True

        return False


    def reset_group(self):
        self.group = ''


    def read_dlg_conf(self, dlgid):
        '''
        En la bd_ose tenemos 3 grupos de tablas: PZ(pozos), TQ(tanques), PQ(presiones y caudales)
        El grupo ya los obtuvimos al buscar el datasource.
        '''
        if self.group == 'PQ':
            d = self.pv_read_dlg_conf_PQ(dlgid)
            return (d)
        elif self.group == 'TQ':
            d = self.pv_read_dlg_conf_TQ(dlgid)
            return (d)
        elif self.group == 'PZ':
            d = self.pv_read_dlg_conf_PZ(dlgid)
            return (d)
        else:
            LOG.info('[%s] ERROR: read_dlg_conf no group !!' % (dlgid))
            return


    def pv_read_dlg_conf_PQ(self, dlgid):
        LOG.info('[%s]read_dlg_conf_PQ' % (dlgid))

        if not self.connect():
            LOG.info('[{0}] ERROR: pv_read_dlg_conf_PQ cant connect bdose.'.format(dlgid))
            return

        query = text("""SELECT timerPoll, timerDial,c0_name, c0_eRange, c0_minValue,c0_maxValue,c0_mag,\
                    c1_name,c1_eRange,c1_minValue,c1_maxValue,c1_mag,c2_name,c2_eRange,c2_minValue,c2_maxValue,c2_mag,\
                    c3_name,c3_eRange,c3_minValue,c3_maxValue,c3_mag,c4_name,c4_eRange,c4_minValue,c4_maxValue,c4_mag,\
                    consigna_mode,consigna_hhmm1,consigna_hhmm2,pwrSaveModo,pwrSaveStartTime,pwrSaveEndTime FROM PQ_tbUnidades WHERE dlgid = '%s'""" % (dlgid))
        try:
            rp = self.conn.execute(query)
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_PQ exec error {0}.'.format(dlgid, errvar))
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
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_PQ split error {0}.'.format(dlgid, errvar))
            imin, imax = 0, 0

        d[('A0', 'IMIN')] = imin
        d[('A0', 'IMAX')] = imax
        d[('A0', 'MMIN')] = result[4]
        d[('A0', 'MMAX')] = result[5]
        # C1=>A1
        d[('A1', 'NAME')] = result[7]
        try:
            imin, imax, *r = re.split('-|mA', result[8])
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_PQ split error {0}.'.format(dlgid, errvar))
            imin, imax = 0, 0

        d[('A1', 'IMIN')] = imin
        d[('A1', 'IMAX')] = imax
        d[('A1', 'MMIN')] = result[9]
        d[('A1', 'MMAX')] = result[10]
        # C2=>A2
        d[('A2', 'NAME')] = result[12]
        try:
            imin, imax, *r = re.split('-|mA', result[13])
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_PQ split error {0}.'.format(dlgid, errvar))
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
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_PQ split error {0}.'.format(dlgid, errvar))
            magpp = 0
        d[('C0', 'MAGPP')] = magpp

        # C4 => C1
        d[('C1', 'NAME')] = result[22]
        try:
            r, magpp = re.split('-', result[23])
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_PQ split error {0}.'.format(dlgid, errvar))
            magpp = 0
        d[('C1', 'MAGPP')] = magpp

        # CONSIGNAS:
        # Hay que adecuar los formatos de HHMM
        cons_enabled = result[27]
        if cons_enabled == 0:
            d[('CONS', 'ENABLED')] = magpp = 'OFF'
        else:
            d[('CONS', 'ENABLED')] = magpp = 'ON'

        try:
            hh, mm, *r = re.split(':', str(result[28]))
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_PQ split error {0}.'.format(dlgid, errvar))
            hh, mm = 0, 0
        d[('CONS', 'HHMM1')] = '%s%s' % (hh, mm)

        try:
            hh, mm, *r = re.split(':', str(result[29]))
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_PQ split error {0}.'.format(dlgid, errvar))
            hh, mm = 0, 0
        d[('CONS', 'HHMM2')] = '%s%s' % (hh, mm)

        # PWRSAVE
        pwr_save_modo = result[30]
        if pwr_save_modo == 0:
            pwr_save_modo = 'OFF'
        else:
            pwr_save_modo = 'ON'
        d[('BASE', 'PWRS_MODO')] = pwr_save_modo

        # result15,16 son timedelta por lo tanto los separo en su representacion str.
        # split me devuleve strings por lo que uso %s%s.
        try:
            hh, mm, *r = re.split(':', str(result[31]))
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_PQ split error {0}.'.format(dlgid, errvar))
            hh, mm = 0, 0
        d[('BASE', 'PWRS_HHMM1')] = '%s%s' % (hh, mm)

        try:
            hh, mm, *r = re.split(':', str(result[32]))
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_PQ split error {0}.'.format(dlgid, errvar))
            hh, mm = 0, 0
        d[('BASE', 'PWRS_HHMM2')] = '%s%s' % (hh, mm)

        return (d)


    def pv_read_dlg_conf_TQ(self, dlgid):
        LOG.info('[%s]read_dlg_conf_TQ' % (dlgid))

        if not self.connect():
            LOG.info('[{0}] ERROR: pv_read_dlg_conf_TQ cant connect bdose.'.format(dlgid))
            return

        query = text("""SELECT timerPoll,timerDial,c0_name,c0_eRange,c0_minValue,c0_maxValue,c1_name,c1_eRange,\
                    c1_minValue,c1_maxValue, c2_name,c2_eRange,c2_minValue,c2_maxValue, pwrSaveModo,pwrSaveStartTime,\
                    pwrSaveEndTime FROM TQ_tbUnidades WHERE dlgid = '%s'""" % (dlgid))


        try:
            rp = self.conn.execute(query)
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_TQ exec error {0}.'.format(dlgid, errvar))
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
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_TQ split error {0}.'.format(dlgid, errvar))
            imin,imax = 0,0
        d[('A0', 'IMIN')] = imin
        d[('A0', 'IMAX')] = imax
        d[('A0', 'MMIN')] = result[4]
        d[('A0', 'MMAX')] = result[5]
        # C1=>A1
        d[('A1', 'NAME')] = result[6]
        try:
            imin, imax, *r = re.split('-|mA', result[7])
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_TQ split error {0}.'.format(dlgid, errvar))
            imin,imax = 0,0
        d[('A1', 'IMIN')] = imin
        d[('A1', 'IMAX')] = imax
        d[('A1', 'MMIN')] = result[8]
        d[('A1', 'MMAX')] = result[9]
        # C2=>A2
        d[('A2', 'NAME')] = result[10]
        try:
            imin, imax, *r = re.split('-|mA', result[11])
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_TQ split error {0}.'.format(dlgid, errvar))
            imin,imax = 0,0
        d[('A2', 'IMIN')] = imin
        d[('A2', 'IMAX')] = imax
        d[('A2', 'MMIN')] = result[12]
        d[('A2', 'MMAX')] = result[13]

        pwr_save_modo = result[14]
        if pwr_save_modo == 0:
            pwr_save_modo = 'OFF'
        else:
            pwr_save_modo = 'ON'
        d[('BASE', 'PWRS_MODO')] = pwr_save_modo

        # result15,16 son timedelta por lo tanto los separo en su representacion str.
        # split me devuleve strings por lo que uso %s%s.
        try:
            hh,mm,*r = re.split(':', str(result[15]))
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_TQ split error {0}.'.format(dlgid, errvar))
            hh,mm = 0,0
        d[('BASE', 'PWRS_HHMM1')] = '%s%s' % (hh,mm)

        try:
            hh,mm,*r = re.split(':', str(result[16]))
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_TQ split error {0}.'.format(dlgid, errvar))
            hh,mm = 0,0
        d[('BASE', 'PWRS_HHMM2')] = '%s%s' % (hh,mm)

        return (d)


    def pv_read_dlg_conf_PZ(self, dlgid):
        LOG.info('[%s]read_dlg_conf_PZ' % (dlgid))

        if not self.connect():
            LOG.info('[{0}] ERROR: pv_read_dlg_conf_PZ cant connect bdose.'.format(dlgid))
            return

        query = text("SELECT timerPoll FROM PZ_tbUnidades WHERE dlgid = '%s'" % (dlgid))
        try:
            rp = self.conn.execute(query)
        except Exception as errvar:
            LOG.info('[{0}] ERROR: read_dlg_conf_TQ exec error {0}.'.format(dlgid, errvar))
            return

        result = rp.first()
        d = defaultdict(dict)
        d[('BASE', 'DIST')] = 'ON'
        d[('BASE', 'TPOLL')] = result
        LOG.info('[%s] BD conf: [BASE][TPOLL]=[%s]' % (dlgid, d[('BASE', 'TPOLL')]))
        return (d)


    def update(self, dlgid, d):

        tb_prefix = self.group
        if tb_prefix not in ('PQ', 'TQ', 'PZ'):
            LOG.info('[%s] ERROR: update BDOSE no group !!' % (dlgid))
            return

        LOG.info('[{0}] update {1}'.format (dlgid, tb_prefix))

        if self.connect() == False:
            LOG.info('[{0}] ERROR: update cant connect bdose.'.format(dlgid))
            return False

        # PASS1: Inserto frame en la tabla de INITS.
        query = text("INSERT INTO {0}_tbInits (dlgId,fechaHora,rcvdFrame,sqe ) VALUES ( '{1}',NOW(),'{2}','{3}' )".format\
                    ( tb_prefix, dlgid, d['RCVDLINE'],d['CSQ']))
        try:
            self.conn.execute(query)
        except Exception as errvar:
            LOG.info('[{0}] ERROR: update exec error {0}.'.format(dlgid, errvar))
            return False

        # PASS2: Actualizo los parametros dinamicos
        query = text("UPDATE {0}_tbUnidades SET version='{1}', imei='{2}', commitedConf=0, ipAddress='{3}' WHERE dlgid='{4}'".format\
                    (tb_prefix, d['FIRMWARE'], d['IMEI'], d['IPADDRESS'], dlgid))
        try:
            self.conn.execute(query)
        except Exception as errvar:
            LOG.info('[{0}] ERROR: update exec error {0}.'.format(dlgid, errvar))
            return False

        return True


    def process_commited_conf(self, dlgid):

        LOG.info('[%s] process_commited_conf' % (dlgid))

        tb_prefix = self.group
        if tb_prefix not in ('PQ', 'TQ', 'PZ'):
            LOG.info('[%s] ERROR: process_commited_conf BDOSE no group !!' % (dlgid))
            return

        LOG.info('[{0}] process_commited_conf {1}'.format(dlgid, tb_prefix))

        if not self.connect():
            LOG.info('[{0}] ERROR: process_commited_conf cant connect bdose.'.format(dlgid))
            return False

        query = text("SELECT commitedConf FROM {0}_tbUnidades WHERE dlgId = '{1}'".format(tb_prefix, dlgid ))
        try:
            rp = self.conn.execute(query)
        except Exception as errvar:
            LOG.info('[{0}] ERROR: process_commited_conf exec error {1}.'.format(dlgid, errvar))
            return

        row = rp.first()
        cc, *rid = row
        LOG.info('[%s] commited_conf [%s]' % (dlgid, cc))
        return (cc)


    def clear_commited_conf(self, dlgid):

        LOG.info('[%s] clear_commited_conf' % (dlgid))

        tb_prefix = self.group
        if tb_prefix not in ('PQ', 'TQ', 'PZ'):
            LOG.info('[%s] ERROR: clear_commited_conf BDOSE no group !!' % (dlgid))
            return

        LOG.info('[{0}] clear_commited_conf {1}'.format(dlgid, tb_prefix))

        if self.connect() == False:
            LOG.info('[{0}] ERROR: clear_commited_conf cant connect bdose.'.format(dlgid))
            return False

        query = text("UPDATE {0}_tbUnidades SET commitedConf = '0' WHERE dlgId = '{1}'".format(tb_prefix, dlgid ))
        print ( query)
        try:
            rp = self.conn.execute(query)
        except Exception as errvar:
            LOG.info('[{0}] ERROR: clear_commited_conf exec error {1}.'.format(dlgid, errvar))
            return

        return


    def insert_data_line(self, dlgid, d):
        if self.group == 'PQ':
            d = self.insert_data_line_PQ(dlgid, d)
        elif self.group == 'TQ':
            d = self.insert_data_line_TQ(dlgid, d)
        elif self.group == 'PZ':
            d = self.insert_data_line_PZ(dlgid, d)
        else:
            LOG.info('[%s] ERROR: insert_data_line no group !!' % (dlgid))
        return


    def insert_data_line_PQ(self, dlgid, d):

        if self.connect() == False:
            LOG.info('process_: ERROR insert_data_line_PQ not connected')
            exit(0)

        insert_str = text("""INSERT INTO PQ_tbDatos ( dlgId,fechaSys,fechaData,pkMonitoreo,pkInstalacion,c0_name,c0_value,\
        c1_name,c1_value,c2_name,c2_value,c5_name,c5_value, rcvdFrame ) VALUES ( '{0}', now(), '{1}',\
        (select pkMonitoreo from PQ_tbInstalaciones where dlgId='{0}}' and status='ACTIVA'),\
        (select pkInstalacion from PQ_tbInstalaciones where dlgId='{0}' and status='ACTIVA'),'pA','{2}','pB','{3}','q0','{4}','bt','{5}','{6}'\
        )""".format(dlgid, d.get('timestamp', '00-00-00 00:00'),d.get('pA', '0'),d.get('pB', '0'),d.get('bt', '0') ,d.get('RCVDLINE', 'err') ))

        try:
            self.conn.execute(insert_str)
        except Exception as error:
            # print ('EXCEPTION: [%s]' % (error) )
            LOG.info('process_: [%s] ERROR insert_data_line_PQ' % (dlgid))
            LOG.info('process_: insert_str [%s]' % (insert_str))

        return


    def insert_data_line_PZ(self, dlgid, d):

        if self.connect() == False:
            LOG.info('process_: ERROR insert_data_line_PZ not connected')
            exit(0)

        insert_str = text("""INSERT IGNORE INTO PZ_tbDatos (pozoId,fechaSys,fechaData,pkMonitoreo,pkInstalacion,c0_name,c0_value, rcvdFrame) \
        	        VALUES ( '{0}', now(), '{1}', (select pkMonitoreo from PZ_tbInstalaciones where pozoId='{0}' and status='ACTIVA'), \
        	        (select pkInstalacion from PZ_tbInstalaciones where pozoId='{0}' and status='ACTIVA'),'H1', {2}, '{3}' \
        	        )""".format(dlgid, d.get('timestamp', '00-00-00 00:00'), d.get('DIST', '-1'), d.get('RCVDLINE', 'err')))

        try:
            self.conn.execute(insert_str)
        except Exception as error:
            # print ('EXCEPTION: [%s]' % (error) )
            LOG.info('process_: [%s] ERROR insert_data_line_PZ' % (dlgid))
            LOG.info('process_: insert_str [%s]' % (insert_str))

        return


    def insert_data_line_TQ(self, dlgid, d):

        if self.connect() == False:
            LOG.info('process_: ERROR insert_data_line_TQ not connected' )
            exit(0)
        insert_str = text("""INSERT IGNORE INTO TQ_tbDatos (tanqueId,fechaSys,fechaData,pkMonitoreo,pkInstalacion,c0_name,c0_value,c1_name,c1_value,rcvdFrame) \
            VALUES ( '{0}', now(), '{1}',(select pkMonitoreo from TQ_tbInstalaciones where tanqueId='{0}' and status='ACTIVA'),\
            (select pkInstalacion from TQ_tbInstalaciones where tanqueId='{0}' and status='ACTIVA'),'H', {2}, 'bt', {3}, '{4}'\
            )""".format( dlgid, d.get('timestamp','00-00-00 00:00'), d.get('H','0'), d.get('bt','0'), d.get('RCVDLINE','err') ) )
        try:
            self.conn.execute(insert_str)
        except Exception as error:
            #print ('EXCEPTION: [%s]' % (error) )
            LOG.info('process_: [%s] ERROR insert_data_line_TQ' % (dlgid ))
            LOG.info('process_: insert_str [%s]' % (insert_str))

        return


    def insert_data_online(self, dlgid, d):
        if self.group == 'PQ':
            d = self.insert_data_online_PQ(dlgid, d)
        elif self.group == 'TQ':
            d = self.insert_data_online_TQ(dlgid, d)
        elif self.group == 'PZ':
            d = self.insert_data_online_PZ(dlgid, d)
        else:
            LOG.info('[%s] ERROR: insert_data_online no group !!' % (dlgid))
        return


    def insert_data_online_PQ(self, dlgid, d):

        if self.connect() == False:
            LOG.info('process_: ERROR insert_data_line_PQ not connected')
            exit(0)

        insert_str = text("""INSERT INTO PQ_tbDatos ( dlgId,fechaSys,fechaData,pkMonitoreo,pkInstalacion,c0_name,c0_value,\
        c1_name,c1_value,c2_name,c2_value,c5_name,c5_value, sqe) VALUES ( '{0}', now(), '{1}',\
        (select pkMonitoreo from PQ_tbInstalaciones where dlgId='{0}}' and status='ACTIVA'),\
        (select pkInstalacion from PQ_tbInstalaciones where dlgId='{0}' and status='ACTIVA'),'pA','{2}','pB','{3}','q0','{4}','bt','{5}',\
        ( select sqe from PQ_tbInits where dlgId='{0}' order by pkInits DESC limit 1)
        )""".format(dlgid, d.get('timestamp', '00-00-00 00:00'),d.get('pA', '0'),d.get('pB', '0'),d.get('bt', '0')  ))

        try:
            self.conn.execute(insert_str)
        except Exception as error:
            # print ('EXCEPTION: [%s]' % (error) )
            LOG.info('process_: [%s] ERROR insert_data_line_PQ' % (dlgid))
            LOG.info('process_: insert_str [%s]' % (insert_str))

        return


    def insert_data_online_PZ(self, dlgid, d):

        if self.connect() == False:
            LOG.info('process_: ERROR insert_data_online_PZ not connected')
            exit(0)

        insert_str = text("""INSERT IGNORE INTO PZ_tbDatosOnline (pozoId,fechaSys,fechaData,pkMonitoreo,pkInstalacion,c0_name,c0_value, sqe) \
        	        VALUES ( '{0}', now(), '{1}', (select pkMonitoreo from PZ_tbInstalaciones where pozoId='{0}' and status='ACTIVA'), \
        	        (select pkInstalacion from PZ_tbInstalaciones where pozoId='{0}' and status='ACTIVA'),'H1', {2}, '{3}',\
        	        ( select sqe from PZ_tbInits where dlgId='{0}}' order by pkInits DESC limit 1)
        	        )""".format(dlgid, d.get('timestamp', '00-00-00 00:00'), d.get('DIST', '-1')))

        try:
            self.conn.execute(insert_str)
        except Exception as error:
            # print ('EXCEPTION: [%s]' % (error) )
            LOG.info('process_: [%s] ERROR insert_data_online_PZ' % (dlgid))
            LOG.info('process_: insert_str [%s]' % (insert_str))

        return


    def insert_data_online_TQ(self, dlgid, d):

        if self.connect() == False:
            LOG.info('process_: ERROR insert_data_online_TQ not connected' )
            exit(0)

        insert_str = text("""INSERT IGNORE INTO TQ_tbDatosOnline (tanqueId,fechaSys,fechaData,pkMonitoreo,pkInstalacion,c0_name,c0_value,c1_name,c1_value,sqe) \
        VALUES ( '{0}', now(), '{1}', (select pkMonitoreo from TQ_tbInstalaciones where tanqueId='{0}' and status='ACTIVA'),\
        (select pkInstalacion from TQ_tbInstalaciones where tanqueId='{0}' and status='ACTIVA'),'H', {2}, 'bt', {3}, \
        ( select sqe from TQ_tbInits where dlgId='{0}' order by pkInits DESC limit 1)\
        )""".format( dlgid, d.get('timestamp','00-00-00 00:00'), d.get('H','0'), d.get('bt','0') ) )

        try:
            self.conn.execute(insert_str)
        except Exception as error:
            #print ('EXCEPTION: [%s]' % (error) )
            LOG.info('process_: [%s] ERROR insert_data_online_TQ' % (dlgid ))
            LOG.info('process_: insert_str [%s]' % (insert_str))

        return


if __name__ == '__main__':

    import spy_log as log
    LOG = log.config_logger()

    bd = BDOSE(modo='local')
    if bd.connect():
        print ('BDose connected !')
    else:
        print ('BDose connect error.')
        exit(1)
    # , 'PZ001','UYRIV013'
    for dlgid in ('SPY001',):

        bd.reset_group()
        bd.find_group(dlgid)
        print('{0} Group = {1}'.format(dlgid, bd.group))
        '''
        d = bd.read_dlg_conf(dlgid)
        if d is not None:
            for key in sorted(d.keys()):
                value = d[key]
                print ('{0}: {1}={2}'.format(dlgid, key, value))

        d = dict()
        d['IPADDRESS'] = '1.2.3.4'
        d['RCVDLINE'] = 'Received Line'
        d['FIRMWARE'] = 'R123'
        d['IMEI'] = 'imei1234567'
        d['CSQ'] = '80'
        d['UID'] = 'uid12345'
        d['SIMID'] = 'simid12345'
        d['COMMITED_CONF'] = 0

        bd.update(dlgid, d)
        '''

        cc = bd.process_commited_conf(dlgid)
        print('{0} commited_conf = {1}'.format(dlgid, cc))
        bd.clear_commited_conf(dlgid)
