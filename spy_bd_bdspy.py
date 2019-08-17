# -*- coding: utf-8 -*-
"""
Modulo de conexion a la BD BDSPY.
Implemento la BD como una clase
En el init me conecto y me quedo con el conector.
Luego, c/metodo me da los parametros que quiera
"""

from sqlalchemy import create_engine
from sqlalchemy import text
from spy import Config
import logging

# Creo un logger local child.
LOG = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
class BDSPY:


    def __init__(self, modo='local'):

        self.datasource = ''
        self.engine = ''
        self.conn = ''
        self.connected = False

        if modo == 'spymovil':
            self.url = Config['BDATOS']['url_bdspy_spymovil']
        elif modo == 'local':
            import pymysql
            pymysql.install_as_MySQLdb()
            self.url = Config['BDATOS']['url_bdspy_local']
        return


    def connect(self):
        """
        Si no estoy conectado a la BD intento conectarme.
        Retorna True/False si es posible generar una conexion a la bd BDSPY
        """
        if self.connected == True:
            return (True)

        try:
            self.engine = create_engine(self.url)
        except:
            self.connected = False
            LOG.info('ERROR: BDSPY engine NOT created')
            exit(1)

        try:
            self.conn = self.engine.connect()
            self.connected = True
            return (True)
        except:
            self.connected = False
            LOG.info('ERROR: BDSPY NOT connected')
            exit(1)

        return (False)


    def dlgIsDefined(self, dlgid):
        """
        Retorna True/False dependiendo si se encontrol el dlgid definido en BDSPY
        BDSPY es la madre de todas las BD ya que es la que indica en que BD tiene
        la unidad definida su configuracion y cual es su datasource
        """

        if self.connect() == False:
            LOG.info('[{0}] ERROR: dlgIsDefined cant connect bdspy.'.format(dlgid))
            return False

        # Vemos si el dlg esta definido en la BDSPY
        query = text("SELECT id FROM spy_equipo WHERE dlgid = '%s'" % (dlgid))
        try:
            rp = self.conn.execute(query)
        except:
            LOG.info('[{0}] ERROR: dlgIsDefined exec error.'.format(dlgid))
            return (False)

        row = rp.first()
        if row is None:  # Veo no tener resultado vacio !!!
            return False
        else:
            return True
        return False


    def uidIsDefined(self, uid):
        """
        Retorna True/False si el uid esta definido en la BDSPY.
        Cuando no encontramos una entrada por dlgid, buscamos por uid y esta es
        la que nos permite reconfigurar el dlgid.
        En este caso guardamos el dlgid en el self para luego ser consultado.
        """
        if self.connect() == False:
            LOG.info('[{0}] ERROR: uidIsDefined cant connect bdspy.'.format(uid))
            return False

        query = text("SELECT dlgid FROM spy_equipo WHERE uid = '%s'" % (uid))
        try:
            rp = self.conn.execute(query)
        except:
            LOG.info('[{0}] ERROR: uidIsDefined exec error.'.format(uid))
            return (False)

        row = rp.first()
        if row is None:
            return False
        else:
            return True

        return False


    def get_dlgid_from_uid(self, uid):

        if self.connect() == False:
            LOG.info('[{0}] ERROR: get_dlgid_from_uid cant connect bdspy.'.format(uid))
            return False

        query = text("SELECT dlgid FROM spy_equipo WHERE uid = '%s'" % (uid))
        try:
            rp = self.conn.execute(query)
        except:
            LOG.info('[{0}] ERROR: get_dlgid_from_uid exec error.'.format(uid))
            return ('')

        row = rp.first()
        self.dlgid = row[0]
        return self.dlgid


    def findDataSource(self, dlgid):
        """
        Determina en que base de datos (GDA/TAHONA/bd_ose) esta definido el dlg
        Retorna True/False
        En la bd_ose tenemos 3 grupos de tablas: PZ(pozos), TQ(tanques), PQ(presiones y caudales)
        En esta instancia si el datasource es bd_ose, determinamos a que grupo pertenece de modo
        de ahorrarle trabajo luego a las funciones de lectura/escritura de la BD.
        Retorna: datasource o None
        """

        # Si ya tengo el datasource no tengo que consultar a la BD.
        if self.datasource == 'GDA' or self.datasource == 'bd_ose':
            return self.datasource

        if not self.connect():
            LOG.info('[{0}] ERROR: findDataSource cant connect bdspy.'.format(dlgid))
            return

        query = text("SELECT datasource FROM spy_equipo WHERE dlgid = '%s'" % (dlgid))
        try:
            rp = self.conn.execute(query)
        except:
            LOG.info('[{0}] ERROR: findDataSource  exec error.'.format(dlgid))
            return

        results = rp.fetchone()
        if results == None:
            LOG.info('[{0}] ERROR: findDataSource  return None.'.format(dlgid))
            return

        self.datasource = results[0]
        LOG.info('[{0}] findDataSource DS={1}'.format(dlgid, self.datasource))

        return self.datasource


    def reset_datasource(self, dlgid):
        self.datasource = ''
        LOG.info('[{0}] reset_dataSource'.format(dlgid))

