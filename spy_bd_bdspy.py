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
from spy_log import log

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
        except Exception as err_var:
            self.connected = False
            log(module=__name__, function='connect', msg='ERROR: BDSPY engine NOT created. ABORT !!')
            log(module=__name__, function='connect', msg='EXCEPTION {}'.format(err_var))
            exit(1)

        try:
            self.conn = self.engine.connect()
            self.connected = True
            return (True)
        except Exception as err_var:
            self.connected = False
            log(module=__name__, function='connect', msg='ERROR: BDSPY NOT connected. ABORT !!')
            log(module=__name__, function='connect', msg='EXCEPTION {}'.format(err_var))
            exit(1)

        return (False)


    def dlg_is_defined(self, dlgid):
        """
        Retorna True/False dependiendo si se encontrol el dlgid definido en BDSPY
        BDSPY es la madre de todas las BD ya que es la que indica en que BD tiene
        la unidad definida su configuracion y cual es su datasource
        """

        if self.connect() == False:
            log(module=__name__, function='dlg_is_defined', dlgid=dlgid, msg='ERROR: can\'t connect bdspy !!')
            return False

        # Vemos si el dlg esta definido en la BDSPY
        query = text("SELECT id FROM spy_equipo WHERE dlgid = '%s'" % (dlgid))
        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='dlg_is_defined', dlgid=dlgid, msg='ERROR: can\'t exec bdspy !!')
            log(module=__name__, function='dlg_is_defined', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            return (False)

        row = rp.first()
        if row is None:  # Veo no tener resultado vacio !!!
            return False
        else:
            return True


    def uid_is_defined(self, uid):
        """
        Retorna True/False si el uid esta definido en la BDSPY.
        Cuando no encontramos una entrada por dlgid, buscamos por uid y esta es
        la que nos permite reconfigurar el dlgid.
        En este caso guardamos el dlgid en el self para luego ser consultado.
        """
        if self.connect() == False:
            log(module=__name__, function='uid_is_defined', msg='ERROR: can\'t connect bdspy !!')
            return False

        query = text("SELECT dlgid FROM spy_equipo WHERE uid = '%s'" % (uid))
        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='uid_is_defined', msg='ERROR: can\'t exec bdspy !!')
            log(module=__name__, function='uid_is_defined', msg='EXCEPTION {}'.format(err_var))
            return False

        row = rp.first()
        if row is None:
            return False
        else:
            return True


    def get_dlgid_from_uid(self, uid):

        if self.connect() == False:
            log(module=__name__, function='get_dlgid_from_uid', msg='ERROR: can\'t connect bdspy !!')
            return False

        query = text("SELECT dlgid FROM spy_equipo WHERE uid = '%s'" % (uid))
        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='get_dlgid_from_uid', msg='ERROR: can\'t exec bdspy !!')
            log(module=__name__, function='get_dlgid_from_uid', msg='EXCEPTION {}'.format(err_var))
            return ('')

        row = rp.first()
        self.dlgid = row[0]
        return self.dlgid


    def find_data_source(self, dlgid):
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
            log(module=__name__, function='find_data_source', dlgid=dlgid, msg='ERROR: can\'t connect bdspy !!')
            return

        query = text("SELECT datasource FROM spy_equipo WHERE dlgid = '%s'" % (dlgid))
        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='find_data_source', dlgid=dlgid, msg='ERROR: can\'t exec bdspy !!')
            log(module=__name__, function='find_data_source', dlgid=dlgid, msg='EXCEPTION {}'.format(err_var))
            return

        results = rp.fetchone()
        if results == None:
            log(module=__name__, function='find_data_source', dlgid=dlgid, msg='ERROR: DS=None')
            return

        self.datasource = results[0]
        log(module=__name__, function='find_data_source', dlgid=dlgid, msg='DS={}'.format(self.datasource))

        return self.datasource


    def reset_datasource(self, dlgid):
        self.datasource = ''
        log(module=__name__, function='reset_datasource', level='SELECT', dlgid=dlgid, msg='start')
