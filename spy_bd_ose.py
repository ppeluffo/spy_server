# -*- coding: utf-8 -*-
"""
Modulo de trabajo con la BD OSE
"""

from sqlalchemy import create_engine
from sqlalchemy import text
from spy import Config
import logging
from collections import defaultdict

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
        except:
            self.connected = False
            LOG.info('ERROR: BDOSE engine NOT created')
            exit(1)

        try:
            self.conn = self.engine.connect()
            self.connected = True
            return (True)
        except:
            self.connected = False
            LOG.info('ERROR: BDOSE NOT connected')
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
        except:
            LOG.info('[{0}] ERROR: find_group(A) exec error.'.format(dlgid))
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
        except:
            LOG.info('[{0}] ERROR: find_group(B) exec error.'.format(dlgid))
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
        except:
            LOG.info('[{0}] ERROR: find_group(C) exec error.'.format(dlgid))
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
            d = self.read_dlg_conf_PQ(dlgid)
            return (d)
        elif self.group == 'TQ':
            d = self.read_dlg_conf_TQ(dlgid)
            return (d)
        elif self.group == 'PZ':
            d = self.read_dlg_conf_PZ(dlgid)
            return (d)
        else:
            LOG.info('[%s] ERROR: read_dlg_conf no group !!' % (dlgid))
            return
        pass


    def read_dlg_conf_from_PQ(self, dlgid):
        pass


    def read_dlg_conf_TQ(self, dlgid):
        pass


    def read_dlg_conf_PZ(self, dlgid):
        LOG.info('[%s]read_dlg_conf_PZ' % (dlgid))

        if not self.connect():
            LOG.info('[{0}] ERROR: pv_read_dlg_conf_from_bdose_PZ cant connect bdose.'.format(dlgid))
            return

        query = text("SELECT timerPoll FROM PZ_tbUnidades WHERE dlgid = '%s'" % (dlgid))
        try:
            rp = self.conn.execute(query)
        except:
            LOG.info('[{0}] ERROR: read_dlg_conf_PZ excec error.'.format(dlgid))
            return

        result = rp.first()
        d = defaultdict(dict)
        d[('BASE', 'DIST')] = 'ON'
        d[('BASE', 'TPOLL')] = result
        LOG.info('[%s] BD conf: [BASE][TPOLL]=[%s]' % (d[('BASE', 'TPOLL')]))
        return (d)


    def update(self, dlgid, d):
        pass


    def process_commited_conf(self, dlgid):
        pass


    def clear_commited_conf(self, dlgid):
        pass


    def insert_data_line(self, dlgid, d):
        pass


    def insert_data_line_online(dlgid, d):
        pass

