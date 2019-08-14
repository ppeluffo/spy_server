# -*- coding: utf-8 -*-
"""
Modulo de conexion a la BD.
Implemento la BD como una clase
En el init me conecto y me quedo con el conector.
Luego, c/metodo me da los parametros que quiera
"""

#import pymysql
from sqlalchemy import create_engine
from sqlalchemy import text
from spy import Config
import logging
from collections import defaultdict

# Creo un logger local child.
LOG = logging.getLogger(__name__)

#------------------------------------------------------------------------------
class BD:
    
    def __init__ (self, modo = 'local'):
        
#         pymysql.install_as_MySQLdb()
         
         self.datasource = ''
         self.engine_gda = ''
         self.conn_bdspy = ''
         self.conn_gda = ''
         self.conn_bdose = ''
         self.bdspy_connected = False
         self.gda_connected = False
         self.bdspy_connected = False
         
         if modo == 'spymovil':
             self.url_gda = Config['BDATOS']['url_gda_spymovil']
             self.url_bdspy = Config['BDATOS']['url_bdspy_spymovil']
             self.url_bdose = Config['BDATOS']['url_bdose_spymovil']
         elif modo == 'local':
             self.url_gda = Config['BDATOS']['url_gda_local']
             self.url_bdspy = Config['BDATOS']['url_bdspy_local']
             self.url_bdose = Config['BDATOS']['url_bdose_local']
         return
 
    
    def connect_gda(self ):
        """
        Retorna True/False si es posible generar una conexion a la bd GDA
        """
        if self.gda_connected == True:
            return(True)
        
        self.engine_gda = create_engine(self.url_gda)
        try:
            self.conn_gda = self.engine_gda.connect()
            self.gda_connected = True
            return(True)
        except:
            self.gda_connected = False
            LOG.info('ERROR: GDA NOT connected')
            exit(1)
            return(False)


    def connect_bdspy(self ):      
        """
        Si no estoy conectado a la BD intento conectarme.
        Retorna True/False si es posible generar una conexion a la bd BDSPY
        """
        if self.bdspy_connected == True:
            return(True)
        
        self.engine_bdspy = create_engine(self.url_bdspy)
        try:
            self.conn_bdspy = self.engine_bdspy.connect()
            self.bdspy_connected = True
            return(True)
        except:
            self.bdspy_connected = False
            LOG.info('ERROR: BDSPY NOT connected')
            exit(1)
            return(False)
 
    
    def connect_bdose(self ):
        """
        Retorna True/False si es posible generar una conexion a la bd_ose
        """
        self.engine_bdose = create_engine(self.url_bdose)
        try:
            self.conn_bdose = self.engine_bdose.connect()
            return(True)
        except:
            return(False)

                                         
    def dlgIsDefined(self, dlgid ):
        """
        Retorna True/False dependiendo si se encontrol el dlgid definido en BDSPY
        BDSPY el la madre de todas las BD ya que es la que indica en que BD tiene
        la unidad definida su configuracion
        """
        self.connect_bdspy()                  
        # Vemos si el dlg esta definido en la BDSPY
        query = text( "SELECT id FROM spy_equipo WHERE dlgid = '%s'" % (dlgid) )
        #query = text( "SELECT id, datasource FROM spy_equipo ")
        #print (query)
        rp = self.conn_bdspy.execute(query)
        row = rp.first()
        if row is None:     # Veo no tener resultado vacio !!!
            return False
        
        return True
 
           
    def uidIsDefined( self, uid ):
        """
        Retorna True/False si el uid esta definido en la BDSPY.
        Cuando no encontramos una entrada por dlgid, buscamos por uid y esta es
        la que nos permite reconfigurar el dlgid.
        En este caso guardamos el dlgid en el self para luego ser consultado.
        """
        self.connect_bdspy()
        query = text( "SELECT dlgid FROM spy_equipo WHERE uid = '%s'" % (uid) )
        #print (query)
        rp = self.conn_bdspy.execute(query)
        row = rp.first()
        if row is None:
            return False

        return True
 
    
    def get_dlgid_from_uid(self, uid):
        self.connect_bdspy()     
        query = text( "SELECT dlgid FROM spy_equipo WHERE uid = '%s'" % (uid) )
        rp = self.conn_bdspy.execute(query)
        row = rp.first()
        self.dlgid = row[0]
        return self.dlgid
 
    
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

        '''
        self.connect_gda()    
        query = text("""SELECT spx_unidades_configuracion.nombre as 'canal', 
                     spx_configuracion_parametros.parametro, spx_configuracion_parametros.value, 
                     spx_configuracion_parametros.configuracion_id as 'param_id' 
                     FROM spx_unidades,spx_unidades_configuracion, spx_tipo_configuracion, spx_configuracion_parametros 
                     WHERE spx_unidades.id = spx_unidades_configuracion.dlgid_id 
                     AND spx_unidades_configuracion.tipo_configuracion_id = spx_tipo_configuracion.id 
                     AND spx_configuracion_parametros.configuracion_id = spx_unidades_configuracion.id 
                     AND spx_unidades_configuracion.nombre = 'PILOTO' AND spx_unidades.dlgid = '%s'""" % (dlgid) )
            
        rp = self.conn_gda.execute(query)
        results = rp.fetchall()
        d = dict()
        for row in results:
            tag, pname, value, pid = row
            d[pname] = value
            
        return(d)
 
       
    def read_dlg_conf_from_bd( self, dlgid):
        # Leo la configuracion 
        LOG.info('[%s] dlg_read_conf_from_bd' % ( dlgid) )   
        self.datasource = self.findDataSource(dlgid)
        LOG.info('[%s] dlg_read_conf_from_bd DS=%s' % ( dlgid, self.datasource) )
       
        if self.datasource == 'GDA':          
            d = self.pv_read_dlg_conf_from_gda(dlgid)
            return(d)
        elif self.datasource == 'bd_ose':
             d = self.pv_read_dlg_conf_from_bdose(dlgid)
        else:
             LOG.info('[%s] dlg_read_conf_from_bd DS=%s NOT implemented' % ( dlgid, self.datasource) )
        return
  
    
    def update(self, dlgid, d):
        # Leo la configuracion 
        self.datasource = self.findDataSource(dlgid)
        LOG.info('[%s] update_bd DS=%s' % ( dlgid, self.datasource) )   
        
        if self.datasource == 'GDA':          
            d = self.pv_update_gda(dlgid, d)
            return(d)
        elif self.datasource == 'bd_ose':
             d = self.pv_update_bdose(dlgid, d)
        else:
             LOG.info('[%s] update_bd DS=%s NOT implemented' % ( dlgid, self.datasource) )
        return


    def process_commited_conf(self, dlgid):
        # Leo la configuracion
        self.datasource = self.findDataSource(dlgid)
        LOG.info('[%s] commitedconf_bd DS=%s' % (dlgid, self.datasource))

        if self.datasource == 'GDA':
            cc = self.pv_process_commited_conf_gda(dlgid)
            return (cc)
        elif self.datasource == 'bd_ose':
            cc = self.pv_process_commited_conf_bdose(dlgid)
            return (cc)
        else:
            LOG.info('[%s] commitedconf_bd DS=%s NOT implemented' % (dlgid, self.datasource))
        return


    def clear_commited_conf(self, dlgid):
        '''
        Reseteo el valor de commited_conf a 0
        '''
        self.datasource = self.findDataSource(dlgid)

        if self.datasource == 'GDA':
            self.pv_clear_commited_conf_gda(dlgid)
        elif self.datasource == 'bd_ose':
            self.pv_clear_commited_conf_bdose(dlgid)
        else:
            LOG.info('[%s] clear_commitedconf_bd DS=%s NOT implemented' % (dlgid, self.datasource))
        return


    def findDataSource( self, dlgid ):
        """
        Determina en que base de datos (GDA/TAHONA/bd_ose) esta definido el dlg
        Retorna el nombre de la BD.
        """
        self.connect_bdspy()
        query = text( "SELECT datasource FROM spy_equipo WHERE dlgid = '%s'" % (dlgid) )
        rp = self.conn_bdspy.execute(query)
        results = rp.fetchone()
        if results == None:
           return
        self.datasource = results[0]
        return(self.datasource)


    def insert_data_line(self, dlgid, d):
        # Leo la configuracion
        self.datasource = self.findDataSource(dlgid)
        #LOG.info('process_: insert_data_line DS=%s' % (self.datasource))

        if self.datasource == 'GDA':
            self.pv_insert_data_line_gda(dlgid, d)
            self.pv_insert_data_line_gda_online(dlgid, d)
            return
        elif self.datasource == 'bd_ose':
            self.pv_insert_data_line_bdose(dlgid, d)
            self.pv_insert_data_line_bdose_online(dlgid, d)
        else:
            LOG.info('process_: insert_data_line DS=%s NOT implemented' % (self.datasource))
        return

    #------------------------------------------------------------------------------
    # FUNCIONES PRIVADAS
    # ------------------------------------------------------------------------------

    def pv_read_dlg_conf_from_gda( self, dlgid ):
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
        
        self.connect_gda() 
        
        query = text("""SELECT spx_unidades_configuracion.nombre as 'canal', spx_configuracion_parametros.parametro, 
                     spx_configuracion_parametros.value, spx_configuracion_parametros.configuracion_id as \"param_id\" FROM spx_unidades,
                     spx_unidades_configuracion, spx_tipo_configuracion, spx_configuracion_parametros 
                     WHERE spx_unidades.id = spx_unidades_configuracion.dlgid_id 
                     AND spx_unidades_configuracion.tipo_configuracion_id = spx_tipo_configuracion.id 
                     AND spx_configuracion_parametros.configuracion_id = spx_unidades_configuracion.id 
                     AND spx_unidades.dlgid = '%s'""" % (dlgid) )
 
        rp = self.conn_gda.execute(query)
        results = rp.fetchall()
        d = defaultdict(dict)
        for row in results:
            canal, pname, value, pid = row
            d[(canal,pname)] = value
            LOG.info('[%s] BD [%s][%s]=[%s]' % ( dlgid, canal, pname, d[(canal,pname)] ) )
            
        return(d)


    def pv_read_dlg_conf_from_bdose( self, dlgid ):
        pass
   
          
    def pv_update_gda(self, dlgid, d):
        
        self.connect_gda() 
        # PASS1: Inserto frame en la tabla de INITS.
        query = text("""INSERT IGNORE INTO spx_inits (fecha,dlgid_id,csq,rxframe) VALUES ( NOW(), \
                     ( SELECT id FROM spx_unidades WHERE dlgid = '%s'), '%s', '%s')""" % (dlgid, d['CSQ'], d['RCVDLINE']) )
        #print(query)
        try:
            self.conn_gda.execute(query)
            LOG.info('[%s] pv_update_gda INIT' % dlgid )
        except:
            LOG.info('[%s] ERROR pv_update_gda' % dlgid )
            
        # PASS2: Actualizo los parametros dinamicos
        for key in d:
            value = d[key]
            query = text("""UPDATE spx_configuracion_parametros SET value = '%s' WHERE parametro = '%s' \
                         AND configuracion_id = ( SELECT id FROM spx_unidades_configuracion WHERE nombre = 'BASE' \
                         AND dlgid_id = ( SELECT id FROM spx_unidades WHERE dlgid = '%s'))""" % ( value, key, dlgid))
            #print(query)
            try:
                self.conn_gda.execute(query)
                LOG.info('[%s] pv_update_gda %s=%s' % (dlgid, key, value ) )
            except:
                LOG.info('[%s] ERROR pv_update_gda %s' % (dlgid, key) )
       
        return
    
    
    def pv_update_bdose(self, dlgid, d):
        pass


    def pv_process_commited_conf_gda(self, dlgid):
        self.connect_gda()
        query = text("""SELECT value, configuracion_id FROM spx_configuracion_parametros WHERE parametro = 'COMMITED_CONF' 
                     AND configuracion_id = ( SELECT id FROM spx_unidades_configuracion WHERE nombre = 'BASE' 
                     AND dlgid_id = ( SELECT id FROM spx_unidades WHERE dlgid = '%s') )""" % (dlgid))

        rp = self.conn_gda.execute(query)
        results = rp.fetchall()
        for row in results:
            cc, rid = row
            LOG.info('[%s] commited_conf_BD [%s]' % (dlgid, cc))
        return (cc)


    def pv_process_commited_conf_bdose(self, dlgid):
        pass


    def pv_clear_commited_conf_gda(self, dlgid):
        self.connect_gda()
        query = text("""UPDATE spx_configuracion_parametros SET value = '0' WHERE parametro = 'COMMITED_CONF' 
                    AND configuracion_id = ( SELECT id FROM spx_unidades_configuracion WHERE nombre = 'BASE' 
                    AND dlgid_id = ( SELECT id FROM spx_unidades WHERE dlgid = '$%s'))""" % (dlgid))
        # print(query)
        try:
            self.conn_gda.execute(query)
        except:
            LOG.info('[%s] ERROR pv_clear_commited_conf_gda %s' % (dlgid))


    def pv_clear_commited_conf_bdose(self, dlgid):
        pass


    def pv_insert_data_line_gda(self, dlgid, d):

        if self.connect_gda() == False:
            LOG.info('process_: ERROR pv_insert_data_line_gda not connected' )
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

            #LOG.info('DEBUG: [%s]->[%s]' % ( key, value))

            try:
                self.conn_gda.execute(query)
                #LOG.info('process_: pv_insert_data_line_gda %s=%s' % ( key, value))
            except Exception as error:
                #print ('EXCEPTION: [%s]' % (error) )
                LOG.info('process_: ERROR pv_insert_data_line_gda [%s], [%s]->[%s]' % (dlgid, key, value))
                LOG.info('process_: QUERY [%s]' % (query))

        return


    def pv_insert_data_line_bdose(self, dlgid, d):
        pass


    def pv_insert_data_line_gda_online(self, dlgid, d):

        if self.connect_gda() == False:
            LOG.info('process_: ERROR pv_insert_data_line_gda_online not connected')
            exit(0)

        for key in d:

            if key == 'timestamp':
                continue

            value = d[key]

            query = text("""INSERT IGNORE INTO spx_online (fechasys, fechadata, valor, medida_id, ubicacion_id ) VALUES ( now(),'{0}','{1}', \
            ( SELECT uc.tipo_configuracion_id FROM spx_unidades AS u JOIN spx_unidades_configuracion AS uc ON uc.dlgid_id = u.id \
            JOIN spx_configuracion_parametros AS cp  ON cp.configuracion_id = uc.id \
            WHERE cp.parametro = 'NAME' AND cp.value = '{2}' AND u.dlgid = '{3}' ), \
            ( SELECT ubicacion_id FROM spx_instalacion WHERE unidad_id = ( SELECT id FROM spx_unidades WHERE dlgid = '{3}')))""".format( d['timestamp'], value, key, dlgid))
            #LOG.info('DEBUG: [%s]->[%s]' % (key, value))
            try:
                self.conn_gda.execute(query)
                #LOG.info('process_: pv_insert_data_line_gda_online %s=%s' % (key, value))
            except Exception as error:
                # print ('EXCEPTION: [%s]' % (error) )
                LOG.info('process_: ERROR pv_insert_data_line_gda_online [%s], [%s]->[%s]' % (dlgid, key, value))
                LOG.info('process_: QUERY [%s]' % (query))

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
                self.conn_gda.execute(delquery)
                #LOG.info('process_: pv_delete_data_line_gda_online %s=%s' % (key, value))
            except Exception as error:
                # print ('EXCEPTION: [%s]' % (error) )
                LOG.info('process_: ERROR pv_delete_data_line_gda_online [%s], [%s]->[%s]' % (dlgid, key, value))
                LOG.info('process_: QUERY [%s]' % (query))

        return


    def pv_insert_data_line_bdose_online(dlgid, d):
        pass
