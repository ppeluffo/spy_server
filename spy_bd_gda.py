# -*- coding: utf-8 -*-
"""
Modulo de trabajo con la BD GDA
"""

from sqlalchemy import create_engine
from sqlalchemy import text
from spy import Config
from collections import defaultdict
from spy_log import log

# ------------------------------------------------------------------------------
class BDGDA:

    def __init__(self, modo='local'):

        self.datasource = ''
        self.engine = ''
        self.conn = ''
        self.connected = False

        if modo == 'spymovil':
            self.url = Config['BDATOS']['url_gda_spymovil']
        elif modo == 'local':
            self.url = Config['BDATOS']['url_gda_local']
        return


    def connect(self):
        """
        Retorna True/False si es posible generar una conexion a la bd GDA
        """
        if self.connected == True:
            return (True)

        try:
            self.engine = create_engine(self.url)
        except Exception as err_var:
            self.connected = False
            log(module=__name__, function='connect', msg='ERROR: GDA engine NOT created. ABORT !!')
            log(module=__name__, function='connect', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))
            exit(1)

        try:
            self.conn = self.engine.connect()
            self.connected = True
            return (True)
        except Exception as err_var:
            self.connected = False
            log(module=__name__, function='connect', msg='ERROR: GDA NOT connected. ABORT !!')
            log(module=__name__, function='connect', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))
            exit(1)

        return (False)


    def read_conf_piloto(self, dlgid):
        '''
        Lee de la base GDA la configuracion de los pilotos para el datalogger dado.
        Retorna un diccionario con los datos.
        La BD devuelve
        +--------+-----------+-------+----------+
        | canal  | parametro | value | param_id |
        +--------+-----------+-------+----------+
        | PILOTO | HHMM_0    | 230   |     1107 |
        | PILOTO | HHMM_2    | 650   |     1107 |
        | PILOTO | POUT_4    | 1.3   |     1107 |
        | PILOTO | POUT_1    | 2.4   |     1107 |
        | PILOTO | POUT_2    | 1.8   |     1107 |
        | PILOTO | POUT_3    | 1.6   |     1107 |
        | PILOTO | HHMM_3    | 1040  |     1107 |
        | PILOTO | PSTEPS    | 6     |     1107 |
        | PILOTO | HHMM_1    | 430   |     1107 |
        | PILOTO | POUT_0    | 1.2   |     1107 |
        | PILOTO | PBAND     | 0.2   |     1107 |
        | PILOTO | HHMM_4    | 1750  |     1107 |
        +--------+-----------+-------+----------+

        Los datos se ponene un un diccionario con key=parametro y este se retorna

        '''
        if self.connect() == False:
            log(module=__name__, function='read_conf_piloto', dlgid=dlgid, msg='ERROR: can\'t connect gda !!')
            return False

        query = text("""SELECT spx_unidades_configuracion.nombre as 'canal', 
                         spx_configuracion_parametros.parametro, spx_configuracion_parametros.value, 
                         spx_configuracion_parametros.configuracion_id as 'param_id' 
                         FROM spx_unidades,spx_unidades_configuracion, spx_tipo_configuracion, spx_configuracion_parametros 
                         WHERE spx_unidades.id = spx_unidades_configuracion.dlgid_id 
                         AND spx_unidades_configuracion.tipo_configuracion_id = spx_tipo_configuracion.id 
                         AND spx_configuracion_parametros.configuracion_id = spx_unidades_configuracion.id 
                         AND spx_unidades_configuracion.nombre = 'PILOTO' AND spx_unidades.dlgid = '%s'""" % (dlgid))

        d = dict()
        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='read_conf_piloto', dlgid=dlgid, msg='ERROR: can\'t exec gda !!')
            log(module=__name__, function='read_conf_piloto', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))
            return (d)

        results = rp.fetchall()
        d = dict()
        for row in results:
            tag, pname, value, pid = row
            d[pname] = value

        return (d)


    def read_dlg_conf(self, dlgid):
        '''
        Leo la configuracion desde GDA
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
        log(module=__name__, function='read_dlg_conf', dlgid=dlgid, level='SELECT', msg='start')

        if self.connect() == False:
            log(module=__name__, function='read_dlg_conf', dlgid=dlgid, msg='ERROR: can\'t connect gda !!')
            return

        query = text("""SELECT spx_unidades_configuracion.nombre as 'canal', spx_configuracion_parametros.parametro, 
                    spx_configuracion_parametros.value, spx_configuracion_parametros.configuracion_id as \"param_id\" FROM spx_unidades,
                    spx_unidades_configuracion, spx_tipo_configuracion, spx_configuracion_parametros 
                    WHERE spx_unidades.id = spx_unidades_configuracion.dlgid_id 
                    AND spx_unidades_configuracion.tipo_configuracion_id = spx_tipo_configuracion.id 
                    AND spx_configuracion_parametros.configuracion_id = spx_unidades_configuracion.id 
                    AND spx_unidades.dlgid = '%s'""" % (dlgid))

        try:
            rp = self.conn.execute(query)
        except:
            log(module=__name__, function='read_dlg_conf', dlgid=dlgid, msg='ERROR: can\'t exec gda !!')
            return

        results = rp.fetchall()
        d = defaultdict(dict)
        for row in results:
            canal, pname, value, pid = row
            d[(canal, pname)] = value
            log(module=__name__, function='read_dlg_conf', dlgid=dlgid, level='SELECT', msg='BD conf: [{0}][{1}]=[{2}]'.format( canal, pname, d[(canal, pname)]))

        return (d)


    def update(self, dlgid, d):

        log(module=__name__, function='update', dlgid=dlgid, level='SELECT', msg='start' )

        if self.connect() == False:
            log(module=__name__, function='update', dlgid=dlgid, msg='ERROR: can\'t connect gda !!')
            return False

        # PASS1: Inserto frame en la tabla de INITS.
        query = text("""INSERT IGNORE INTO spx_inits (fecha,dlgid_id,csq,rxframe) VALUES ( NOW(), \
                     ( SELECT id FROM spx_unidades WHERE dlgid = '%s'), '%s', '%s')""" % (dlgid, d['CSQ'], d['RCVDLINE']))

        try:
            self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='update', dlgid=dlgid, msg='ERROR: can\'t exec(1) gda !!')
            log(module=__name__, function='update', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))
            return

        # PASS2: Actualizo los parametros dinamicos
        for key in d:
            value = d[key]
            query = text("""UPDATE spx_configuracion_parametros SET value = '%s' WHERE parametro = '%s' \
                         AND configuracion_id = ( SELECT id FROM spx_unidades_configuracion WHERE nombre = 'BASE' \
                         AND dlgid_id = ( SELECT id FROM spx_unidades WHERE dlgid = '%s'))""" % (value, key, dlgid))
            try:
                self.conn.execute(query)
            except Exception as err_var:
                log(module=__name__, function='update', dlgid=dlgid, msg='ERROR: can\'t exec(2) gda !!')
                log(module=__name__, function='connect', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))
                return

            log(module=__name__, function='update', dlgid=dlgid, level='SELECT', msg='{0}={1}'.format(key,value))

        return


    def process_commited_conf(self, dlgid):
        '''
        :param dlgid:
        :return: valor del commited_conf de la BD o None en caso de errores
        '''
        log(module=__name__, function='process_commited_conf', dlgid=dlgid, level='SELECT', msg='start')

        if self.connect() == False:
            log(module=__name__, function='process_commited_conf', dlgid=dlgid, msg='ERROR: can\'t connect gda !!')
            return

        query = text("""SELECT value, configuracion_id FROM spx_configuracion_parametros WHERE parametro = 'COMMITED_CONF' 
                     AND configuracion_id = ( SELECT id FROM spx_unidades_configuracion WHERE nombre = 'BASE' 
                     AND dlgid_id = ( SELECT id FROM spx_unidades WHERE dlgid = '%s') )""" % (dlgid))

        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='process_commited_conf', dlgid=dlgid, msg='ERROR: can\'t exec gda !!')
            log(module=__name__, function='process_commited_conf', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))
            return

        #row = rp.first()
        cc, rid = rp.first()
        log(module=__name__, function='process_commited_conf', dlgid=dlgid, msg='cc={}'.format(cc))
        return (cc)


    def clear_commited_conf(self, dlgid):

        log(module=__name__, function='clear_commited_conf', dlgid=dlgid, level='SELECT', msg='start')

        if self.connect() == False:
            log(module=__name__, function='clear_commited_conf', dlgid=dlgid, msg='ERROR: can\'t connect gda !!')
            return

        query = text("""UPDATE spx_configuracion_parametros SET value = '0' WHERE parametro = 'COMMITED_CONF' 
                    AND configuracion_id = ( SELECT id FROM spx_unidades_configuracion WHERE nombre = 'BASE' 
                    AND dlgid_id = ( SELECT id FROM spx_unidades WHERE dlgid = '$%s'))""" % (dlgid))
        # print(query)
        try:
            self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, function='clear_commited_conf', dlgid=dlgid, msg='ERROR: can\'t exec gda !!')
            log(module=__name__, function='clear_commited_conf', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))

        return


    def insert_data_line(self, dlgid, d):

        if self.connect() == False:
            log(module=__name__, function='insert_data_line', dlgid=dlgid, msg='ERROR: can\'t connect gda !!')
            exit(0)

        for key in d:
            if key == 'timestamp':
                continue
            value = d[key]
            query = text("""INSERT IGNORE INTO spx_datos (fechasys, fechadata, valor, medida_id, ubicacion_id ) VALUES \
                         ( now(),'{0}','{1}',( SELECT uc.tipo_configuracion_id FROM spx_unidades AS u JOIN spx_unidades_configuracion \
                         AS uc ON uc.dlgid_id = u.id JOIN spx_configuracion_parametros AS cp  ON cp.configuracion_id = uc.id WHERE \
                         cp.parametro = 'NAME' AND cp.value = '{2}' AND u.dlgid = '{3}' ),( SELECT ubicacion_id FROM spx_instalacion \
                         WHERE unidad_id = ( SELECT id FROM spx_unidades WHERE dlgid = '{3}')))""".format( d['timestamp'], value, key, dlgid ))

            try:
                self.conn.execute(query)
            except Exception as err_var:
                log(module=__name__, function='insert_data_line', dlgid=dlgid, msg='ERROR: [{0}]->[{1}]'.format(key,value))
                log(module=__name__, function='insert_data_line', dlgid=dlgid,msg='QUERY {}'.format(query))
                log(module=__name__, function='insert_data_line', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))

        return


    def insert_data_online(self, dlgid, d):

        if self.connect() == False:
            log(module=__name__, function='insert_data_online', dlgid=dlgid, msg='ERROR: can\'t connect gda !!')
            return

        for key in d:

            if key == 'timestamp':
                continue

            value = d[key]

            query = text("""INSERT IGNORE INTO spx_online (fechasys, fechadata, valor, medida_id, ubicacion_id ) VALUES ( now(),'{0}','{1}', \
            ( SELECT uc.tipo_configuracion_id FROM spx_unidades AS u JOIN spx_unidades_configuracion AS uc ON uc.dlgid_id = u.id \
            JOIN spx_configuracion_parametros AS cp  ON cp.configuracion_id = uc.id \
            WHERE cp.parametro = 'NAME' AND cp.value = '{2}' AND u.dlgid = '{3}' ), \
            ( SELECT ubicacion_id FROM spx_instalacion WHERE unidad_id = ( SELECT id FROM spx_unidades WHERE dlgid = '{3}')))""".format( d['timestamp'], value, key, dlgid))

            try:
                self.conn.execute(query)
            except Exception as err_var:
                log(module=__name__, function='insert_data_online', dlgid=dlgid, msg='ERROR: [{0}]->[{1}]'.format(key,value))
                log(module=__name__, function='insert_data_online', dlgid=dlgid,msg='QUERY {}'.format(query))
                log(module=__name__, function='insert_data_online', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))

            delquery = text("""DELETE FROM spx_online  WHERE medida_id = ( SELECT uc.tipo_configuracion_id FROM spx_unidades AS u \
            JOIN spx_unidades_configuracion AS uc ON uc.dlgid_id = u.id JOIN spx_configuracion_parametros AS cp  ON cp.configuracion_id = uc.id \
            WHERE cp.parametro = 'NAME' AND cp.value = '{0}' AND u.dlgid = '{1}' ) AND ubicacion_id = \
            ( SELECT ubicacion_id FROM spx_instalacion WHERE unidad_id = ( SELECT id FROM spx_unidades WHERE dlgid = '{1}')) \
            AND id NOT IN( SELECT * FROM (SELECT id FROM spx_online WHERE medida_id = ( SELECT uc.tipo_configuracion_id FROM spx_unidades \
            AS u JOIN spx_unidades_configuracion AS uc ON uc.dlgid_id = u.id JOIN spx_configuracion_parametros AS cp  \
            ON cp.configuracion_id = uc.id WHERE cp.parametro = 'NAME' AND cp.value = '{0}' AND u.dlgid = '{1}' ) \
            AND ubicacion_id = ( SELECT ubicacion_id FROM spx_instalacion WHERE unidad_id = \
            ( SELECT id FROM spx_unidades WHERE dlgid = '{1}')) ORDER BY id DESC LIMIT 1) AS temp )""".format( key, dlgid) )

            # LOG.info('DEBUG: [%s]->[%s]' % (key, value))
            try:
                self.conn.execute(delquery)
                #LOG.info('process_: delete_data_line_online %s=%s' % (key, value))
            except Exception as err_var:
                # print ('EXCEPTION: [%s]' % (error) )
                log(module=__name__, function='insert_data_online', dlgid=dlgid, msg='ERROR: DELETE [{0}]->[{1}]'.format(key,value))
                log(module=__name__, function='insert_data_online', dlgid=dlgid,msg='QUERY: DELETE {}'.format(query))
                log(module=__name__, function='insert_data_online', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))

        return


