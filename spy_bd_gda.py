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
class BDGDA:

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
            self.url = Config['BDATOS']['url_gda_ute']
        return


    def connect(self, tag='GDA'):
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


    def read_piloto_conf(self, dlgid):
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
        log(module=__name__, server=self.server, function='read_piloto_conf', dlgid=dlgid, level='SELECT', msg='start')
        if not self.connect():
            log(module=__name__, server=self.server, function='read_piloto_conf', dlgid=dlgid, msg='ERROR: can\'t connect gda !!')
            return False

        sql = """SELECT spx_unidades_configuracion.nombre as 'canal', 
                         spx_configuracion_parametros.parametro, spx_configuracion_parametros.value, 
                         spx_configuracion_parametros.configuracion_id as 'param_id' 
                         FROM spx_unidades,spx_unidades_configuracion, spx_tipo_configuracion, spx_configuracion_parametros 
                         WHERE spx_unidades.id = spx_unidades_configuracion.dlgid_id 
                         AND spx_unidades_configuracion.tipo_configuracion_id = spx_tipo_configuracion.id 
                         AND spx_configuracion_parametros.configuracion_id = spx_unidades_configuracion.id 
                         AND spx_unidades_configuracion.nombre = 'PILOTO' AND spx_unidades.dlgid = '{}'""".format(dlgid)
        try:
            query = text(sql)
        except Exception as err_var:
            log(module=__name__, server=self.server, function='read_piloto_conf', dlgid=dlgid, msg='ERROR: SQLQUERY: {}'.format(sql))
            log(module=__name__, server=self.server, function='read_piloto_conf', dlgid=dlgid, msg='ERROR: EXCEPTION {}'.format(err_var))
            return False

        d = dict()
        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, server=self.server, function='read_piloto_conf', dlgid=dlgid,msg='ERROR: GDA exec EXCEPTION {}'.format(err_var))
            return False

        results = rp.fetchall()
        d = dict()
        for row in results:
            tag, pname, value, pid = row
            d[pname] = value

        return d


    def read_dlg_conf(self, dlgid, tag='GDA'):
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
        log(module=__name__, server=self.server, function='read_dlg_conf', dlgid=dlgid, level='SELECT', msg='start_{}'.format(tag))

        if not self.connect(tag):
            log(module=__name__, server=self.server, function='read_dlg_conf', dlgid=dlgid, msg='ERROR_{}: can\'t connect !!'.format(tag))
            return

        sql = """SELECT spx_unidades_configuracion.nombre as 'canal', spx_configuracion_parametros.parametro, 
                    spx_configuracion_parametros.value, spx_configuracion_parametros.configuracion_id as \"param_id\" FROM spx_unidades,
                    spx_unidades_configuracion, spx_tipo_configuracion, spx_configuracion_parametros 
                    WHERE spx_unidades.id = spx_unidades_configuracion.dlgid_id 
                    AND spx_unidades_configuracion.tipo_configuracion_id = spx_tipo_configuracion.id 
                    AND spx_configuracion_parametros.configuracion_id = spx_unidades_configuracion.id 
                    AND spx_unidades.dlgid = '{}'""".format (dlgid)
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
            canal, pname, value, pid = row
            d[(canal, pname)] = value
            log(module=__name__, server=self.server, function='read_dlg_conf', dlgid=dlgid, level='SELECT', msg='BD_{0} conf: [{1}][{2}]=[{3}]'.format( tag,canal, pname, d[(canal, pname)]))

        return d


    def update(self, dlgid, d, tag='GDA'):

        log(module=__name__, server=self.server, function='update', dlgid=dlgid, level='SELECT', msg='start_{}'.format(tag) )

        if not self.connect():
            log(module=__name__, server=self.server, function='update', dlgid=dlgid, msg='ERROR_{}: can\'t connect !!'.format(tag))
            return False

        # PASS1: Inserto frame en la tabla de INITS.
        sql = """INSERT IGNORE INTO spx_inits (fecha,dlgid_id,csq,rxframe) VALUES ( NOW(), \
                     ( SELECT id FROM spx_unidades WHERE dlgid = '{0}'), '{1}', '{2}')""" .format(dlgid, d['CSQ'], d['RCVDLINE'])
        try:
            query = text(sql)
            #log(module=__name__, server=self.server, function='DEBUG update', dlgid=dlgid, level='SELECT', msg='query[{}]'.format(query))
        except Exception as err_var:
            log(module=__name__, server=self.server, function='update', dlgid=dlgid, msg='ERROR_{0}: SQLQUERY: {1}'.format(tag, sql))
            log(module=__name__, server=self.server, function='update', dlgid=dlgid, msg='ERROR_{0}: EXCEPTION {1}'.format(tag, err_var))
            return False

        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, server=self.server, function='update', dlgid=dlgid,msg='ERROR_{}: exec EXCEPTION {}'.format(tag, err_var))
            return False


        # PASS2: Actualizo los parametros dinamicos
        for key in d:
            value = d[key]
            sql = """UPDATE spx_configuracion_parametros SET value = '{0}' WHERE parametro = '{1}' \
                         AND configuracion_id = ( SELECT id FROM spx_unidades_configuracion WHERE nombre = 'BASE' \
                         AND dlgid_id = ( SELECT id FROM spx_unidades WHERE dlgid = '{2}'))""".format(value, key, dlgid)
            try:
                query = text(sql)
            except Exception as err_var:
                log(module=__name__, server=self.server, function='update', dlgid=dlgid,msg='ERROR_{0}: SQLQUERY: {1}'.format(tag, sql))
                log(module=__name__, server=self.server, function='update', dlgid=dlgid,msg='ERROR_{0}: EXCEPTION {1}'.format(tag, err_var))
                return False

            try:
                rp = self.conn.execute(query)
            except Exception as err_var:
                log(module=__name__, server=self.server, function='update', dlgid=dlgid,
                    msg='ERROR_{}: exec EXCEPTION {}'.format(tag, err_var))
                return False

            log(module=__name__, server=self.server, function='update', dlgid=dlgid, level='SELECT', msg='{0}: {1}={2}'.format(tag, key,value))

        return


    def process_commited_conf(self, dlgid, tag='GDA'):
        '''
        :param dlgid:
        :return: valor del commited_conf de la BD o None en caso de errores
        '''
        log(module=__name__, function='process_commited_conf', dlgid=dlgid, level='SELECT', msg='start')

        if not self.connect():
            log(module=__name__, function='process_commited_conf', dlgid=dlgid, msg='ERROR: can\'t connect {0} !!'.format(tag))
            return

        sql = """SELECT value, configuracion_id FROM spx_configuracion_parametros WHERE parametro = 'COMMITED_CONF' 
                     AND configuracion_id = ( SELECT id FROM spx_unidades_configuracion WHERE nombre = 'BASE' 
                     AND dlgid_id = ( SELECT id FROM spx_unidades WHERE dlgid = '{}') )""".format(dlgid)
        try:
            query = text(sql)
        except Exception as err_var:
            log(module=__name__, server=self.server, function='process_commited_conf', dlgid=dlgid, msg='ERROR_{0}: SQLQUERY: {1}'.format(tag, sql))
            log(module=__name__, server=self.server, function='process_commited_conf', dlgid=dlgid, msg='ERROR_{0}: EXCEPTION {1}'.format(tag, err_var))
            return False

        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, server=self.server, function='process_commited_conf', dlgid=dlgid,msg='ERROR_{}: exec EXCEPTION {}'.format(tag, err_var))
            return False
        #row = rp.first()
        cc, rid = rp.first()
        log(module=__name__, function='process_commited_conf', dlgid=dlgid, msg='{0}: cc={1}'.format(tag, cc))
        return (cc)


    def clear_commited_conf(self, dlgid, tag='GDA'):

        log(module=__name__, function='clear_commited_conf', dlgid=dlgid, level='SELECT', msg='start_{}'.format(tag))

        if not self.connect():
            log(module=__name__, function='clear_commited_conf', dlgid=dlgid, msg='ERROR_{0}: can\'t connect !!'.format(tag))
            return
        sql = """UPDATE spx_configuracion_parametros SET value = '0' WHERE parametro = 'COMMITED_CONF' 
                    AND configuracion_id = ( SELECT id FROM spx_unidades_configuracion WHERE nombre = 'BASE' 
                    AND dlgid_id = ( SELECT id FROM spx_unidades WHERE dlgid = '{}'))""".format(dlgid)
        try:
            query = text(sql)
        except Exception as err_var:
            log(module=__name__, server=self.server, function='clear_commited_conf', dlgid=dlgid, msg='ERROR_{0}: SQLQUERY: {1}'.format(tag, sql))
            log(module=__name__, server=self.server, function='clear_commited_conf', dlgid=dlgid, msg='ERROR_{0}: EXCEPTION {1}'.format(tag, err_var))
            return False

        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            log(module=__name__, server=self.server, function='clear_commited_conf', dlgid=dlgid,msg='ERROR_{}: exec EXCEPTION {}'.format(tag, err_var))
            return False

        return True


    def insert_data_line(self, dlgid, d, tag='GDA'):

        if not self.connect():
            log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid, msg='ERROR_{}: can\'t connect!!'.format(tag))
            exit(0)

        errors = 0
        for key in d:
            if key == 'timestamp' or key == 'RCVDLINE':
                continue
            value = d[key]
            log(module=__name__, server=self.server, function='insert_data_line', level='SELECT', dlgid=dlgid, msg='DEBUG_{0} {1}->{2}'.format(tag, key, value))
            sql = """INSERT INTO spx_datos (fechasys, fechadata, valor, medida_id, ubicacion_id ) VALUES \
                         ( now(),'{0}','{1}',( SELECT uc.tipo_configuracion_id FROM spx_unidades AS u JOIN spx_unidades_configuracion \
                         AS uc ON uc.dlgid_id = u.id JOIN spx_configuracion_parametros AS cp  ON cp.configuracion_id = uc.id WHERE \
                         cp.parametro = 'NAME' AND cp.value = '{2}' AND u.dlgid = '{3}' ),( SELECT ubicacion_id FROM spx_instalacion \
                         WHERE unidad_id = ( SELECT id FROM spx_unidades WHERE dlgid = '{3}')))""".format( d['timestamp'], value, key, dlgid )
            #print('DEBUG:  {}'.format(sql))
            #continue
            try:
                query = text(sql)
            except Exception as err_var:
                log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid, msg='ERROR_{0}: SQLQUERY: {1}'.format(tag, sql))
                log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid, msg='ERROR_{0}: EXCEPTION {1}'.format(tag, err_var))
                errors += 1
                continue

            try:
                rp = self.conn.execute(query)
            except Exception as err_var:
                if 'Duplicate entry' in str(err_var):
                    # Los duplicados no hacen nada malo. Se da mucho en testing.
                    log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid, msg='WARN_{}: Duplicated Key'.format(tag))
                    continue
                else:
                    log(module=__name__, server=self.server, function='insert_data_line', dlgid=dlgid,msg='ERROR_{}: exec EXCEPTION {}'.format(tag, err_var))
                    errors += 1
                    continue

        #print('DEBUG ERRORS={}'.format(errors))
        if errors > 0:
            return False
        else:
            return True


    def insert_data_online(self, dlgid, d, tag='GDA'):

        if not self.connect():
            log(module=__name__, server=self.server, function='insert_data_online', dlgid=dlgid, msg='ERROR_{}: can\'t connect !!'.format(tag))
            return

        errors = 0
        for key in d:
            if key == 'timestamp' or key == 'RCVDLINE':
                continue
            value = d[key]
            log(module=__name__, server=self.server, function='insert_data_online', level='SELECT', dlgid=dlgid,msg='DEBUG_{0} {1}->{2}'.format(tag, key, value))
            sql = """INSERT INTO spx_online (fechasys, fechadata, valor, medida_id, ubicacion_id ) VALUES ( now(),'{0}','{1}', \
            ( SELECT uc.tipo_configuracion_id FROM spx_unidades AS u JOIN spx_unidades_configuracion AS uc ON uc.dlgid_id = u.id \
            JOIN spx_configuracion_parametros AS cp  ON cp.configuracion_id = uc.id \
            WHERE cp.parametro = 'NAME' AND cp.value = '{2}' AND u.dlgid = '{3}' ), \
            ( SELECT ubicacion_id FROM spx_instalacion WHERE unidad_id = ( SELECT id FROM spx_unidades WHERE dlgid = '{3}')))""".format( d['timestamp'], value, key, dlgid)
            #print(sql)
            try:
                query = text(sql)
            except Exception as err_var:
                log(module=__name__, server=self.server, function='insert_data_online', dlgid=dlgid, msg='ERROR_{0}: SQLQUERY: {1}'.format(tag, sql))
                log(module=__name__, server=self.server, function='insert_data_online', dlgid=dlgid, msg='ERROR_{0}: EXCEPTION {1}'.format(tag, err_var))
                errors += 1
                continue

            try:
                rp = self.conn.execute(query)
            except Exception as err_var:
                if 'Duplicate entry' in str(err_var):
                    log(module=__name__, server=self.server, function='insert_data_online', dlgid=dlgid, msg='WARN_{}: Duplicated Key'.format(tag))
                    errors += 1
                else:
                    log(module=__name__, server=self.server, function='insert_data_online', dlgid=dlgid,msg='ERROR_{}: exec EXCEPTION {}'.format(tag, err_var))
                    errors += 1

            sql = """DELETE FROM spx_online  WHERE medida_id = ( SELECT uc.tipo_configuracion_id FROM spx_unidades AS u \
            JOIN spx_unidades_configuracion AS uc ON uc.dlgid_id = u.id JOIN spx_configuracion_parametros AS cp  ON cp.configuracion_id = uc.id \
            WHERE cp.parametro = 'NAME' AND cp.value = '{0}' AND u.dlgid = '{1}' ) AND ubicacion_id = \
            ( SELECT ubicacion_id FROM spx_instalacion WHERE unidad_id = ( SELECT id FROM spx_unidades WHERE dlgid = '{1}')) \
            AND id NOT IN( SELECT * FROM (SELECT id FROM spx_online WHERE medida_id = ( SELECT uc.tipo_configuracion_id FROM spx_unidades \
            AS u JOIN spx_unidades_configuracion AS uc ON uc.dlgid_id = u.id JOIN spx_configuracion_parametros AS cp  \
            ON cp.configuracion_id = uc.id WHERE cp.parametro = 'NAME' AND cp.value = '{0}' AND u.dlgid = '{1}' ) \
            AND ubicacion_id = ( SELECT ubicacion_id FROM spx_instalacion WHERE unidad_id = \
            ( SELECT id FROM spx_unidades WHERE dlgid = '{1}')) ORDER BY id DESC LIMIT 1) AS temp )""".format( key, dlgid)
            try:
                query = text(sql)
            except Exception as err_var:
                log(module=__name__, server=self.server, function='insert_data_online', dlgid=dlgid, msg='ERROR_{0}: SQLQUERY: {1}'.format(tag, sql))
                log(module=__name__, server=self.server, function='insert_data_online', dlgid=dlgid, msg='ERROR_{0}: EXCEPTION {1}'.format(tag, err_var))
                errors += 1
                continue

            try:
                rp = self.conn.execute(query)
            except Exception as err_var:
                log(module=__name__, server=self.server, function='insert_data_online', dlgid=dlgid, msg='ERROR_{}: exec EXCEPTION {}'.format(tag, err_var))
                errors += 1
                continue

        if errors > 0:
            return False
        else:
            return True


class BDGDA_TAHONA(BDGDA):

    def __init__(self, modo='local',server='comms'):

        self.datasource = ''
        self.engine = ''
        self.conn = ''
        self.connected = False
        self.server = server

        if modo == 'spymovil':
            self.url = Config['BDATOS']['url_gda_tahona_spymovil']
        elif modo == 'local':
            self.url = Config['BDATOS']['url_gda_tahona_local']
        return


    def connect(self, tag='GDA_TAHONA'):
        return BDGDA.connect(self, tag=tag )


    def read_conf_piloto(self, dlgid):
         d = dict()
         log(module=__name__, function='read_conf_piloto', dlgid=dlgid, msg='ERROR: can\'t exec gda_tahona !!')
         return d


    def read_dlg_conf(self, dlgid, tag='GDA_TAHONA'):
        return BDGDA.read_dlg_conf(self, dlgid, tag=tag)


    def update(self, dlgid, d, tag='GDA_TAHONA'):
        return BDGDA.update(self, dlgid, d, tag=tag)


    def process_commited_conf(self, dlgid, tag='GDA_TAHONA'):
        return BDGDA.process_commited_conf(self, dlgid, tag=tag)


    def clear_commited_conf(self, dlgid, tag='GDA_TAHONA'):
        return BDGDA.clear_commited_conf(self, dlgid, tag=tag)


    def insert_data_line(self, dlgid, d, tag='GDA_TAHONA'):
        return BDGDA.insert_data_line(self, dlgid, d, tag=tag)


    def insert_data_online(self, dlgid, d, tag='GDA_TAHONA'):
        return BDGDA.insert_data_online(self, dlgid, d, tag=tag)


if __name__ == '__main__':
    bd = BDGDA_TAHONA()
    res = bd.connect('TH')
    print('RES=[{}]'.format(res))
